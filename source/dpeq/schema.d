/**
Structures that describe the schema of query results.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.schema;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.meta;
import std.range;

import dpeq.exceptions;
import dpeq.connection;
import dpeq.constants;
import dpeq.marshalling;



/// Lazily-demarshalled field (column) description
struct FieldDescription
{
    @property string name()
    {
        // from c-string to d-string, no allocation
        return demarshalString(m_buf[0..nameLength-1]);
    }

    /// If the field can be identified as a column of a specific table,
    /// the object ID of the table; otherwise zero.
    @property ObjectID table()
    {
        return demarshalNumber(m_buf[nameLength .. nameLength + 4]);
    }

    /// If the field can be identified as a column of a specific table,
    /// the attribute number of the column; otherwise zero.
    @property short columnId()
    {
        return demarshalNumber!short(m_buf[nameLength + 4 .. nameLength + 6]);
    }

    /// The object ID of the field's data type.
    @property ObjectID type()
    {
        return demarshalNumber(m_buf[nameLength + 6 .. nameLength + 10]);
    }

    /// The data type size (see pg_type.typlen).
    /// Note that negative values denote variable-width types.
    @property short typeLen()
    {
        return demarshalNumber!short(m_buf[nameLength + 10 .. nameLength + 12]);
    }

    /// The type modifier (see pg_attribute.atttypmod).
    /// The meaning of the modifier is type-specific.
    @property int typeModifier()
    {
        return demarshalNumber(m_buf[nameLength + 12 .. nameLength + 16]);
    }

    /// The format code being used for the field. Currently will be zero (text)
    /// or one (binary). In a RowDescription returned from the statement variant
    /// of Describe, the format code is not yet known and will always be zero.
    @property FormatCode formatCode()
    {
        return cast(FormatCode)
            demarshalNumber!short(m_buf[nameLength + 16 .. nameLength + 18]);
    }

    /// backing buffer, owned by Message
    const(ubyte)[] m_buf;

    /// length of name C-string wich spans the head of backing buffer
    private int nameLength;

    static FieldDescription demarshal(const(ubyte)[] buf, out int bytesDiscarded)
    {
        int bytesRead = 0;
        while (buf[bytesRead])  // name is C-string, so it ends with zero
            bytesRead++;
        int nameLength = bytesRead + 1;
        bytesDiscarded = (nameLength + 2 * ObjectID.sizeof + 2 * short.sizeof +
            int.sizeof + FormatCode.sizeof).to!int;
        return FieldDescription(buf[0 .. bytesDiscarded], nameLength);
    }
}


struct RowDescription
{
    /// number of fields (columns) in a row
    @property short fieldCount()
    {
        return demarshalNumber!short(m_buf[0 .. 2]);
    }

    /// buffer owned by Message
    const(ubyte)[] m_buf;

    /// true when row description of this row block was received
    @property bool isSet() const { return m_buf !is null; }

    /// Slice operator, wich returns InputRange of lazily-demarshalled FieldDescriptions.
    auto opIndex()
    {
        static struct FieldDescrRange
        {
            private const(ubyte)[] buf;

            this(const(ubyte)[] backing)
            {
                buf = backing;
            }

            @property bool empty() const
            {
                return buf.length == 0 && !frontDemarshalled;
            }

            private bool frontDemarshalled = false;
            private FieldDescription _front;

            @property FieldDescription front()
            {
                if (frontDemarshalled)
                    return _front;
                assert(buf.length > 0);
                int shift = 0;
                _front = FieldDescription.demarshal(buf, shift);
                buf = buf[shift .. $];
                frontDemarshalled = true;
                return _front;
            }

            void popFront()
            {
                frontDemarshalled = false;
            }
        }

        static assert (isInputRange!FieldDescrRange);

        return FieldDescrRange(m_buf[2..$]);
    }
}


/** Array of rows, returned by the server, wich all share one row
description. Simple queries may include multiple SQL statements, each
corresponding to row block. In extended query protocol flow, row block
is retuned for each "Execute" message. */
struct RowBlock
{
    RowDescription rowDesc;
    Message[] dataRows;

    /// set when the server responded with EmptyQueryResponse to sql query
    /// this row block represents.
    bool emptyQuery;

    /** Set when the server has sent PortalSuspended due to reaching nonzero
    result-row count limit, requested in Execute message. The appearance of
    this message tells the frontend that another Execute should be issued
    against the same portal to complete the operation. */
    bool suspended;
}


/// Generic query result, returned by getQueryResults
struct QueryResult
{
    /// Number of CommandComplete\EmptyQueryResponse\PortalSuspended messages received.
    short commandsComplete;

    /// Data blocks, each block being an array of rows sharing one row
    /// description (schema). Each sql statement in simple query protocol
    /// creates one block. Each portal execution in EQ protocol creates
    /// one block.
    RowBlock[] blocks;

    /// returns true if there is not a single data row in the response.
    @property bool noDataRows() const
    {
        foreach (block; blocks)
            if (block.dataRows.length > 0)
                return false;
        return true;
    }
}


/// NotificationResponse message received from the backend.
/// https://www.postgresql.org/docs/current/static/sql-notify.html
struct Notification
{
    /// The process ID of the notifying backend process.
    int procId;

    /// The name of the channel that the notify has been raised on.
    string channel;

    /// The "payload" string passed from the notifying process.
    string payload;
}


/// Contents of NoticeResponse or ErrorResponse messages.
/// https://www.postgresql.org/docs/current/static/protocol-error-fields.html
struct Notice
{
    /** Field contents are ERROR, FATAL, or PANIC (in an error message), or
    WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a localized
    translation of one of these. Always present. */
    string severity;

    /** Field contents are ERROR, FATAL, or PANIC (in an error
    message), or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message).
    This is identical to the 'severity' field except that the contents are never
    localized. This is present only in messages generated by PostgreSQL
    versions 9.6 and later. */
    string severityV;

    /// https://www.postgresql.org/docs/current/static/errcodes-appendix.html
    char[5] code;

    /// Primary human-readable error message. This should be accurate
    /// but terse (typically one line). Always present.
    string message;

    /// Optional secondary error message carrying more detail about the
    /// problem. Might run to multiple lines.
    string detail;

    /** Optional suggestion what to do about the problem. This is intended to
    differ from Detail in that it offers advice (potentially inappropriate)
    rather than hard facts. Might run to multiple lines. */
    string hint;

    /** Decimal ASCII integer, indicating an error cursor position as an index
    into the original query string. The first character has index 1, and
    positions are measured in characters not bytes. */
    string position;

    /** this is defined the same as the position field, but it is used when
    the cursor position refers to an internally generated command rather than
    the one submitted by the client. The q field will always appear when this
    field appears.*/
    string internalPos;

    /// Text of a failed internally-generated command. This could be, for
    /// example, a SQL query issued by a PL/pgSQL function.
    string internalQuery;

    /** Context in which the error occurred. Presently this includes a call
    stack traceback of active procedural language functions and
    internally-generated queries. The trace is one entry per line, most
    recent first. */
    string where;

    string schema;
    string table;
    string column;

    /// If the error was associated with a specific data type, the name of
    /// the data type.
    string dataType;

    /** If the error was associated with a specific constraint, the name of the
    constraint. Refer to fields listed above for the associated table or domain.
    (For this purpose, indexes are treated as constraints, even if they weren't
    created with constraint syntax.) */
    string constraint;

    /// File name of the source-code location where the error was reported.
    string file;

    /// Line number of the source-code location where the error was reported.
    string line;

    /// Name of the source-code routine reporting the error.
    string routine;
}