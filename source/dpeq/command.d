/**
Commands of various nature.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.command;

import std.algorithm: max;
import std.exception: enforce;
import std.format: format;
import std.conv: to;
import std.traits;
import std.range;
import std.variant;
import std.meta;
import std.typecons;

import dpeq.exceptions;
import dpeq.connection;
import dpeq.constants;
import dpeq.marshalling;
import dpeq.schema;


/////////////////////////////////////
// Different forms of command input
/////////////////////////////////////

/// Simple query is simple. Sent string to server and get responses.
/// The most versatile, unsafe way to issue commands to PSQL. It is also slow.
/// Simple query always returns data in FormatCode.Text format.
void postSimpleQuery(ConnT)(ConnT conn, string query)
{
    conn.putQueryMessage(query);
}


/////////////////////////////////////
// Functions to get query results
/////////////////////////////////////

/// Generic dynamic method, suitable for both simple and prepared queries.
QueryResult getQueryResults(ConnT)(ConnT conn)
{
    QueryResult res;

    bool interceptor(Message msg, ref bool err, ref string errMsg)
    {
        with (BackendMessageType)
        switch (msg.type)
        {
            case EmptyQueryResponse:
                res.empty = true;
                break;
            case CommandComplete:
                res.commandsComplete++;
                break;
            case RowDescription:
                RowBlock rb;
                rb.rowDesc = dpeq.schema.RowDescription(msg.data);
                res.blocks ~= rb;
                break;
            case DataRow:
                if (res.blocks.length == 0)
                {
                    err = true;
                    errMsg ~= "Got row without row description ";
                }
                else
                    res.blocks[$-1].dataRows ~= msg; // we simply save raw bytes
                break;
            default:
                break;
        }
        return false;
    }

    conn.pollMessages(&interceptor, false);
    return res;
}




/////////////////////////////////////////////////////////////////
// Functions used to transform query results to native data types
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
            int len = demarshalNumber(buf[0 .. 4]);
            const(ubyte)[] vbuf = buf[4 .. max(4, len + 4)];
            //writeln(types[column], " ", buf);
            res = Converter.demarshal(vbuf, types[column], fcodes[column], len);
            buf = buf[max(4, len + 4) .. $];
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




/// for row spec `spec` build native tuple representation.
/// Plugin!FieldSpec must evaluate to native type if you want it to work.
template TupleBuilder(FieldSpec[] spec, alias Demarshaller)
{
    alias TupleBuilder =
        Tuple!(
            staticMap!(
                SpecMapper!(Demarshaller).Func,
                aliasSeqOf!spec));
}

template SpecMapper(alias Demarshaller)
{
    template Func(FieldSpec spec)
    {
        static if (is(Demarshaller!spec.type))
            alias Func = Demarshaller!spec.type;
        else
            static assert(0, "Demarshaller doesn't support typeId " ~
                spec.typeId.to!string);
    }
}


/// Returns RandomAccessRange of tuples.
auto blockToTuples(FieldSpec[] spec,
    alias Demarshaller = DefaultFieldMarshaller)(RowBlock block)
{
    alias ResTuple = TupleBuilder!(spec, Demarshaller);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);

    short totalColumns = block.rowDesc.fieldCount;
    enforce!PsqlClientException(totalColumns == spec.length,
        "Expected %d columnts in a row, got %d".format(spec.length, totalColumns));

    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description demarshalling happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        fcArr[i] = fdesc.formatCode;
        ObjectID colType = fdesc.type;
        enforce!PsqlClientException(colType == spec[i].typeId,
            "Colunm %d type mismatch: expected %d, got %d".format(
                i, spec[i].typeId, colType));
        i++;
    }

    //import std.stdio;

    static ResTuple demarshalRow(const(ubyte)[] from, const(FormatCode)[] fcodes)
    {
        ResTuple res;
        int len = 0;
        const(ubyte)[] vbuf;
        from = from[2 .. $];    // skip 16 bytes
        foreach (i, colSpec; aliasSeqOf!(spec))
        {
            len = demarshalNumber(from[0 .. 4]);
            //writeln("col ", i, ", len = ", len, " from = ", from);
            vbuf = from[4 .. max(4, len + 4)];
            res[i] = Demarshaller!(colSpec).demarshal(vbuf, fcodes[i], len);
            from = from[max(4, len + 4) .. $];
        }
        enforce!PsqlClientException(from.length == 0,
            "%d bytes left in supposedly emtied row".format(from.length));
        return res;
    }

    struct RowsRange
    {
    private:
        Message[] dataRows;
        FormatCode[] fcodes;
    public:
        @property size_t length() { return dataRows.length; }
        @property bool empty() { return dataRows.empty; }
        @property ResTuple front()
        {
            return demarshalRow(dataRows[0].data, fcodes);
        }
        @property ResTuple back()
        {
            return demarshalRow(dataRows[$-1].data, fcodes);
        }
        ResTuple opIndex(size_t i)
        {
            return demarshalRow(dataRows[i].data, fcodes);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows, fcodes);
        }
    }

    return RowsRange(block.dataRows, fcArr);
}
