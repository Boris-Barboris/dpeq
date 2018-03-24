/**
Structures that describe query results and notifications, received from backend.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.result;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.meta;
import std.range;

import dpeq.exceptions;
import dpeq.connection;
import dpeq.constants;
import dpeq.serialize;


/// Message, received from backend.
struct Message
{
    BackendMessageType type;
    /// Raw unprocessed message byte array excluding message type byte and
    /// first 4 bytes that represent message body length.
    immutable(ubyte)[] data;
}


/// Lazily-deserialized field (column) description
struct FieldDescription
{
    @safe pure:

    @property string name() const
    {
        // from c-string to d-string, no allocation
        return deserializeString(m_buf[0..nameLength-1]);
    }

    /// If the field can be identified as a column of a specific table,
    /// the object ID of the table; otherwise zero.
    @property OID table() const
    {
        return deserializeNumber(m_buf[nameLength .. nameLength + 4]);
    }

    /// If the field can be identified as a column of a specific table,
    /// the attribute number of the column; otherwise zero.
    @property short columnId() const
    {
        return deserializeNumber!short(m_buf[nameLength + 4 .. nameLength + 6]);
    }

    /// The object ID of the field's data type.
    @property OID type() const
    {
        return deserializeNumber(m_buf[nameLength + 6 .. nameLength + 10]);
    }

    /// The data type size (see pg_type.typlen).
    /// Note that negative values denote variable-width types.
    @property short typeLen() const
    {
        return deserializeNumber!short(m_buf[nameLength + 10 .. nameLength + 12]);
    }

    /// The type modifier (see pg_attribute.atttypmod).
    /// The meaning of the modifier is type-specific.
    @property int typeModifier() const
    {
        return deserializeNumber(m_buf[nameLength + 12 .. nameLength + 16]);
    }

    /// The format code being used for the field. Currently will be zero (text)
    /// or one (binary). In a RowDescription returned from the statement variant
    /// of Describe, the format code is not yet known and will always be zero.
    @property FormatCode formatCode() const
    {
        return cast(FormatCode)
            deserializeNumber!short(m_buf[nameLength + 16 .. nameLength + 18]);
    }

    /// backing buffer, owned by Message
    immutable(ubyte)[] m_buf;

    /// length of name C-string wich spans the head of backing buffer
    private int nameLength;

    static FieldDescription deserialize(immutable(ubyte)[] buf, out int bytesDiscarded)
    {
        int bytesRead = 0;
        while (buf[bytesRead])  // name is C-string, so it ends with zero
            bytesRead++;
        int nameLength = bytesRead + 1;
        bytesDiscarded = (nameLength + 2 * OID.sizeof + 2 * short.sizeof +
            int.sizeof + FormatCode.sizeof).to!int;
        return FieldDescription(buf[0 .. bytesDiscarded], nameLength);
    }
}


struct RowDescription
{
    @safe pure:

    /// number of fields (columns) in a row
    @property short fieldCount() const
    {
        return deserializeNumber!short(m_buf[0 .. 2]);
    }

    /// buffer owned by Message
    immutable(ubyte)[] m_buf;

    /// true when row description of this row block was received
    @property bool isSet() const { return m_buf !is null; }

    /// Slice operator, wich returns ForwardRange of FieldDescriptions.
    auto opIndex() const
    {
        assert(isSet(), "RowDescription is not set");

        static struct FieldDescrRange
        {
            private immutable(ubyte)[] buf;

            this(immutable(ubyte)[] backing)
            {
                buf = backing;
            }

            @property bool empty() const
            {
                return buf.length == 0 && !frontDeserialized;
            }

            private bool frontDeserialized = false;
            private FieldDescription _front;

            @property FieldDescription front()
            {
                if (frontDeserialized)
                    return _front;
                assert(buf.length > 0);
                int shift = 0;
                _front = FieldDescription.deserialize(buf, shift);
                buf = buf[shift .. $];
                frontDeserialized = true;
                return _front;
            }

            void popFront()
            {
                frontDeserialized = false;
            }

            FieldDescrRange save() const
            {
                return this;
            }
        }

        static assert (isForwardRange!FieldDescrRange);

        return FieldDescrRange(m_buf[2..$]);
    }
}


/// Row block data rows are in one of these states
enum RowBlockState: byte
{
    invalid = 0,    /// Default state, wich means it was never set.
    complete,       /// Last data row was succeeded with CommandComplete.
    emptyQuery,     /// EmptyQueryResponse was issued from backend.
    /** Happens when the server has sent PortalSuspended due to reaching nonzero
    result-row count limit, requested in Execute message. The appearance of
    this message tells the frontend that another Execute should be issued
    against the same portal to complete the operation. Keep in mind, that all
    portals are destroyed at the end of transaction, wich means that you
    should not carelessly send Sync message before receiving CommandComplete
    when you use portal suspension functionality and implicit transaction scope
    (no explicit BEGIN\COMMIT). */
    suspended,
    /** Polling stopped early on the client side, for example 'getOneRowBlock'
    stopped because of rowCountLimit. */
    incomplete
}


/** Array of rows, returned by the server, wich all share one row
description. Simple queries may include multiple SQL statements, each
returning a row block. In extended query protocol flow, row block
is retuned for each "Execute" message. */
struct RowBlock
{
    RowDescription rowDesc;
    Message[] dataRows;
    RowBlockState state;

    /**
    The command tag. Present when CommandComplete was received.
    This is usually a single word that identifies which SQL command was completed.
    For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted.
        oid is the object ID of the inserted row if rows is 1 and the target table
        has OIDs; otherwise oid is 0.
    For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.
    For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.
    For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.
    For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.
    For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.
    For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note:
        the row count appears only in PostgreSQL 8.2 and later.) */
    string commandTag;
}


/// Generic query result, returned by getQueryResults
struct QueryResult
{
    /** Data blocks, each block being an array of rows sharing one row
    description. Each sql statement in simple query protocol
    creates one block. Each portal execution in EQ protocol creates one block. */
    RowBlock[] blocks;

    /// returns true if there is not a single data row in the response.
    bool noDataRows() const pure @safe
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


void parseNoticeMessage(immutable(ubyte)[] data, ref Notice n) @safe pure
{
    void copyTillZero(ref string dest)
    {
        size_t length;
        dest = deserializeProtocolString(data, length);
        data = data[length..$];
    }

    void discardTillZero()
    {
        size_t idx = 0;
        while (idx < data.length && data[idx])
            idx++;
        data = data[idx+1..$];
    }

    while (data.length > 1)
    {
        char fieldType = cast(char) data[0];
        data = data[1..$];
        // https://www.postgresql.org/docs/current/static/protocol-error-fields.html
        switch (fieldType)
        {
            case 'S':
                copyTillZero(n.severity);
                break;
            case 'V':
                copyTillZero(n.severityV);
                break;
            case 'C':
                assert(data.length >= 6, "Expected 5 bytes of SQLSTATE code.");
                n.code[] = cast(immutable(char)[]) data[0..5];
                data = data[6..$];
                break;
            case 'M':
                copyTillZero(n.message);
                break;
            case 'D':
                copyTillZero(n.detail);
                break;
            case 'H':
                copyTillZero(n.hint);
                break;
            case 'P':
                copyTillZero(n.position);
                break;
            case 'p':
                copyTillZero(n.internalPos);
                break;
            case 'q':
                copyTillZero(n.internalQuery);
                break;
            case 'W':
                copyTillZero(n.where);
                break;
            case 's':
                copyTillZero(n.schema);
                break;
            case 't':
                copyTillZero(n.table);
                break;
            case 'c':
                copyTillZero(n.column);
                break;
            case 'd':
                copyTillZero(n.dataType);
                break;
            case 'n':
                copyTillZero(n.constraint);
                break;
            case 'F':
                copyTillZero(n.file);
                break;
            case 'L':
                copyTillZero(n.line);
                break;
            case 'R':
                copyTillZero(n.routine);
                break;
            default:
                discardTillZero();
        }
    }
}