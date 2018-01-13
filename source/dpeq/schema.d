/**
Structures that describe a schema of query results.

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



/// Lazily-demarshalled field description
struct FieldDescription
{
    @property string name()
    {
        // from c-string to d-string, no allocation
        return demarshalString(m_buf, nameLength - 1);
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

            @property bool empty()
            {
                return (buf.length == 0 && !frontDemarshalled);
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


/// Array of rows, returned by the server, wich all share one row
/// description. Simple queries may include multiple SELECTs, wich will return
/// multiple blocks of rows.
struct RowBlock
{
    RowDescription rowDesc;
    Message[] dataRows;
}
