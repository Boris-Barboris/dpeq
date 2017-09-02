/**
Schema elements.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.schema;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.variant;
import std.meta;
import std.typecons;
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
    /// number of fields in a row
    @property short fieldCount()
    {
        return demarshalNumber!short(m_buf[0 .. 2]);
    }

    /// buffer owned by Message
    const(ubyte)[] m_buf;

    auto opIndex()
    {
        struct FieldDescrRange
        {
            private const(ubyte)[] buf;

            this(const(ubyte)[] backing)
            {
                buf = backing;
            }

            @property bool empty()
            {
                return (buf.length == 0 && !inited);
            }

            private bool inited = false;
            private FieldDescription _front;

            @property FieldDescription front()
            {
                if (inited)
                    return _front;
                assert(buf.length > 0);
                int shift = 0;
                _front = FieldDescription.demarshal(buf, shift);
                buf = buf[shift .. $];
                inited = true;
                return _front;
            }

            void popFront()
            {
                inited = false;
            }
        }

        static assert (isInputRange!FieldDescrRange);

        return FieldDescrRange(m_buf[2..$]);
    }
}


/// Simple queries may include multiple SELECTs, wich will return
/// multiple row types.
struct RowBlock
{
    RowDescription rowDesc;
    Message[] dataRows;
}


/// Generic query result
struct QueryResult
{
    /// Set if EmptyQueryResponse was met
    bool empty;

    /// Number of CommandComplete messages recieved. Mostly used
    /// in simple query workflow, since extended protocol uses only
    /// ReadyForQuery.
    short commandsComplete;

    /// data
    RowBlock[] blocks;
}



/////////////////////////////////////////////////////////////////
// Methods used to transform query results to native data types
/////////////////////////////////////////////////////////////////


//import std.stdio;

/// Returns RandomAccessRange of InputRanges of variants.
/// Customizable with Converter alias, wich must provide demarshal call.
/// Look into marshalling.DefaultConverter for examples.
auto blockToVariants(alias Converter = NopedDefaultConverter)(RowBlock block)
{
    short totalColumns = block.rowDesc.fieldCount;
    ObjectID[] typeArr = new ObjectID[totalColumns];
    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description demarshalling happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        fcArr[i] = fdesc.formatCode;
        typeArr[i++] = fdesc.type;
    }

    struct RowDemarshaller
    {
    private:
        short column = 0;
        short totalCols;
        const(ubyte)[] buf;
        const(ObjectID)[] types;
        const(FormatCode)[] fcodes;
        bool parsed = false;
        Variant res;
    public:
        @property bool empty() { return column >= totalCols; }
        void popFront()
        {
            parsed = false;
            column++;
        }
        @property Variant front()
        {
            if (parsed)
                return res;
            if (column == 0)
            {
                // we need to skip field count in the start of DataRow message
                buf = buf[2 .. $];
            }
            assert(buf.length > 0);
            int shift;
            //writeln(types[column], " ", buf);
            res = Converter.demarshal(buf, types[column], fcodes[column], shift);
            buf = buf[shift .. $];
            parsed = true;
            return res;
        }
    }

    struct RowsRange
    {
    private:
        Message[] dataRows;
        ObjectID[] columnTypes;
        FormatCode[] fcodes;
        short totalColumns;
    public:
        @property size_t length() { return dataRows.length; }
        @property bool empty() { return dataRows.empty; }
        @property RowDemarshaller front()
        {
            return RowDemarshaller(0, totalColumns, dataRows[0].data,
                columnTypes, fcodes);
        }
        @property RowDemarshaller back()
        {
            return RowDemarshaller(0, totalColumns, dataRows[$-1].data,
                columnTypes, fcodes);
        }
        RowDemarshaller opIndex(size_t i)
        {
            return RowDemarshaller(0, totalColumns, dataRows[i].data,
                columnTypes, fcodes);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows, columnTypes, fcodes, totalColumns);
        }
    }

    return RowsRange(block.dataRows, typeArr, fcArr, totalColumns);
}


/// for row spec `spec` build native tuple representation
template DefaultTupleBuilder(FieldSpec[] spec, alias Plugin = NopSpecConverterPlugin)
{
    alias DefaultTupleBuilder =
        Tuple!(
            staticMap!(
                SpecMapper!(Plugin).Func,
                aliasSeqOf!spec));
}

template SpecMapper(alias Plugin)
{
    template Func(FieldSpec spec)
    {
        pragma(msg, "avavav ", spec);
        static if (is(Plugin!spec))
            alias Func = Plugin!spec;
        else
            alias Func = DefaultSpecConverter!spec;
    }
}

template DefaultSpecConverter(FieldSpec spec)
{
    alias DefaultSpecConverter = TypeByFieldSpec!spec;
}

template NopSpecConverterPlugin(FieldSpec spec)
{
}



/// Returns RandomAccessRange of InputRanges of tuples.
/// Customizable with Converter alias, wich must provide demarshal call.
auto blockToTuples(FieldSpec[] spec, alias Demarshaller = DefaultFieldDemarshaller,
    alias TupleBuilder = DefaultTupleBuilder)()
{
    pragma(msg, "Tuple from spec: ", TupleBuilder!spec);
}
