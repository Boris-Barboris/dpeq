/**
Commands of various nature.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.command;

import std.algorithm: max, map;
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


/*
/////////////////////////////////////
// Different forms of command input
/////////////////////////////////////
*/

/** Simple query is simple. Send string to server and get responses.
The most versatile, but unsafe way to issue commands to PSQL.
Simple query always returns data in FormatCode.Text format.
Simple queries SHOULD NOT be accompanied by SYNC message, they
trigger ReadyForQuery message anyways. 

Every postSimpleQuery or PSQLConnection.sync MUST be accompanied by getQueryResults
call. */
void postSimpleQuery(ConnT)(ConnT conn, string query)
{
    conn.putQueryMessage(query);
}


/// Pre-parsed paramethrized command.
class PreparedStatement(ConnT)
{
    protected
    {
        const(ObjectID)[] paramTypes;
        string query;
        bool parseRequested;
        ConnT conn;
        string parsedName;  // name, reserved for this statement in PSQL connection
        short m_paramCount;
    }

    @property string preparedName() const { return parsedName; }

    @property short paramCount() const { return m_paramCount; }

    /**
    Quoting https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html:

    The number of parameter data types specified (can be zero). Note that this is not an
    indication of the number of parameters that might appear in the query string,
    only the number that the frontend wants to prespecify types for.

    Then, for each parameter, there is the following:

    Int32
        Specifies the object ID of the parameter data type. Placing a zero here is
        equivalent to leaving the type unspecified.

    That means you can leave paramTypes null.
    */
    this(ConnT conn, string query, short paramCount, const(ObjectID)[] paramTypes = null, bool persist = true)
    {
        assert(conn);
        assert(query);
        assert(paramCount >= 0);
        this.conn = conn;
        this.query = query;
        this.paramTypes = paramTypes;
        this.m_paramCount = paramCount;
        if (persist)
            parsedName = conn.getNewPreparedName();
        else
            parsedName = "";
    }

    /// you're not supposed to reparse persistent Prepared statement (it will break
    /// existing portals), create a new one.
    void postParseMessage()
    {
        conn.putParseMessage(parsedName, query, paramTypes[]);
        parseRequested = true;
    }

    alias parse = postParseMessage;

    /// explicit close for persistent prepared statements
    void postCloseMessage()
    {
        assert(parseRequested);
        assert(parsedName.length, "no need to close unnamed prepared statements");
        /** An unnamed prepared statement lasts only until the next Parse
        statement specifying the unnamed statement as destination is issued.
        (Note that a simple Query message also destroys the unnamed statement.) */
        conn.putCloseMessage('S', parsedName);
        parseRequested = false;
    }

    /// poll message queue and make sure parse was completed
    void ensureParseComplete()
    {
        bool parsed = false;
        bool interceptor(Message msg, ref bool err, ref string errMsg)
        {
            with (BackendMessageType)
            switch (msg.type)
            {
                case ParseComplete:
                    parsed = true;
                    return true;
                default:
                    break;
            }
            return false;
        }
        conn.pollMessages(&interceptor, true);
        enforce!PsqlClientException(parsed, "Parse failed");
    }
}


/// Instance of set of parameters bound to prepared statement
class Portal(ConnT)
{
    protected
    {
        PreparedStatement!ConnT prepStmt;
        ConnT conn;
        string portalName;  // name, reserved for this portal in PSQL connection
        bool bindRequested = false;
    }

    this(PreparedStatement!ConnT ps, bool persist = true)
    {
        assert(ps);
        this.conn = ps.conn;
        prepStmt = ps;
        if (persist)
            portalName = conn.getNewPortalName();
        else
            portalName = "";
    }

    void bind(
            FieldSpec[] specs,
            FormatCode[] resCodes,
            alias Marshaller = DefaultFieldMarshaller,
            Args...)
        (in Args args)
    {
        assert(prepStmt.paramCount == Args.length);
        assert(prepStmt.paramCount == specs.length);

        auto safePoint = conn.saveBuffer();
        scope (failure) safePoint.restore();

        if (bindRequested && portalName.length)
            postCloseMessage();

        // TODO: if possible, verify spec against parameter types of prepStmt

        enum fcodesr = [staticMap!(FCodeOfFSpec!(Marshaller).F, aliasSeqOf!specs)];

        alias DlgT = int delegate(ubyte[]);
        DlgT[specs.length] marshallers;
        foreach(i, paramSpec; aliasSeqOf!specs)
        {
            marshallers[i] =
                (ubyte[] to) => Marshaller!paramSpec.marshal(to, args[i]);
        }
        conn.putBindMessage(portalName, prepStmt.parsedName, fcodesr,
            marshallers, resCodes);
        bindRequested = true;
    }

    /// Generic InputRanges of types and field marshallers, to pass them
    /// directly to putBindMessage. No validation performed.
    void bind(FR, PR, RR)(scope FR paramCodeRange, scope PR paramMarshRange,
        scope RR returnCodeRange)
    {
        auto safePoint = conn.saveBuffer();
        scope (failure) safePoint.restore();
        if (bindRequested && portalName.length)
            postCloseMessage();
        conn.putBindMessage(portalName, prepStmt.parsedName, paramCodeRange,
            paramMarshRange, returnCodeRange);
        bindRequested = true;
    }

    /// explicit close for persistent prepared statements
    void postCloseMessage()
    {
        assert(bindRequested);
        assert(portalName.length, "no need to close unnamed portals");
        /**
        If successfully created, a named portal object lasts till the end of the
        current transaction, unless explicitly destroyed. An unnamed portal is
        destroyed at the end of the transaction, or as soon as the next Bind
        statement specifying the unnamed portal as destination is issued.
        (Note that a simple Query message also destroys the unnamed portal.)
        Named portals must be explicitly closed before they can be redefined
        by another Bind message, but this is not required for the unnamed portal.
        */
        conn.putCloseMessage('P', portalName);
        bindRequested = false;
    }

    /// poll message queue and make sure bind was completed
    void ensureBindComplete()
    {
        bool is_bound = false;
        bool interceptor(Message msg, ref bool err, ref string errMsg)
        {
            with (BackendMessageType)
            switch (msg.type)
            {
                case BindComplete:
                    is_bound = true;
                    return true;
                default:
                    break;
            }
            return false;
        }
        conn.pollMessages(&interceptor, true);
        enforce!PsqlClientException(is_bound, "Bind failed");
    }

    /** Send Describe+Execute command.
    If describe is false, no RowDescription message will be requested
    from PSQL - useful for optimistic statically-typed querying. */
    void execute(bool describe = true)
    {
        assert(bindRequested);
        if (describe)
            conn.putDescribeMessage('P', portalName);
        conn.putExecuteMessage(portalName);
    }
}


/*
/////////////////////////////////////
// Functions to get query results
/////////////////////////////////////
*/

/// Generic query result, returned by getQueryResults
struct QueryResult
{
    /// Set if EmptyQueryResponse message was met
    bool empty;

    /// Number of CommandComplete messages received. Mostly used
    /// in simple query workflow, since extended protocol uses only
    /// ReadyForQuery.
    short commandsComplete;

    /// Data blocks, each block being an array of rows sharing one row description (schema).
    RowBlock[] blocks;
}


/// Generic method, suitable for both simple and prepared queries.
QueryResult getQueryResults(ConnT)(ConnT conn, bool requireRowDescription = false)
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
                // RowDescription always starts new row block
                RowBlock rb;
                rb.rowDesc = dpeq.schema.RowDescription(msg.data);
                res.blocks ~= rb;
                break;
            case DataRow:
                if (res.blocks.length == 0)
                {
                    if (requireRowDescription)
                    {
                        err = true;
                        errMsg ~= "Got row without row description ";
                    }
                    res.blocks ~= RowBlock();
                }
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



/*
/////////////////////////////////////////////////////////////////
// Functions used to transform query results to native data types
/////////////////////////////////////////////////////////////////
*/

//import std.stdio;

/// Returns RandomAccessRange of InputRanges of lazy-demarshalled variants.
/// Specific flavor of Variant is derived from Converter.demarshal call return type.
/// Look into marshalling.VariantConverter for demarshal implementation examples.
/// Will append parsed field descriptions to fieldDescs array if passed.
auto blockToVariants(alias Converter = VariantConverter!DefaultFieldMarshaller)
    (RowBlock block, FieldDescription[]* fieldDescs = null)
{
    alias VariantT = ReturnType!(Converter.demarshal);

    enforce!PsqlClientException(block.rowDesc.isSet,
        "Cannot demarshal RowBlock without row description. " ~
        "Did you send describe message?");
    short totalColumns = block.rowDesc.fieldCount;
    ObjectID[] typeArr = new ObjectID[totalColumns];
    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description demarshalling happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        if (fieldDescs)
            (*fieldDescs)[i] = fdesc;
        fcArr[i] = fdesc.formatCode;
        typeArr[i++] = fdesc.type;
    }

    static struct RowDemarshaller
    {
    private:
        short column = 0;
        short totalCols;
        const(ubyte)[] buf;
        const(ObjectID)[] types;
        const(FormatCode)[] fcodes;
        bool parsed = false;

        // cache result to prevent repeated demarshalling on
        // front() call.
        VariantT res;
    public:
        @property bool empty() { return column >= totalCols; }
        void popFront()
        {
            parsed = false;
            column++;
        }
        @property VariantT front()
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

    static struct RowsRange
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
template TupleForSpec(FieldSpec[] spec, alias Demarshaller = DefaultFieldMarshaller)
{
    alias TupleForSpec =
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


/// Returns RandomAccessRange of lazy-demarshalled tuples.
/// Customazable with Demarshaller template.
/// Will append parsed field descriptions to fieldDescs array if passed.
auto blockToTuples
    (FieldSpec[] spec, alias Demarshaller = DefaultFieldMarshaller)
    (RowBlock block, FieldDescription[]* fieldDescs = null)
{
    alias ResTuple = TupleForSpec!(spec, Demarshaller);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);
    enforce!PsqlClientException(block.rowDesc.isSet,
        "Cannot demarshal RowBlock without row description. " ~
        "Did you send describe message?");
    short totalColumns = block.rowDesc.fieldCount;
    enforce!PsqlClientException(totalColumns == spec.length,
        "Expected %d columnts in a row, got %d".format(spec.length, totalColumns));
    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description demarshalling happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        if (fieldDescs)
            (*fieldDescs)[i] = fdesc;
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
            "%d bytes left in supposedly emptied row".format(from.length));
        return res;
    }

    static struct RowsRange
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


class FormatCodesOfSpec(FieldSpec[] spec, alias Demarshaller)
{
    static const(FormatCode)[spec.length] codes;

    static this()
    {
        foreach (i, fpec; aliasSeqOf!spec)
            codes[i] = Demarshaller!fpec.formatCode;
    }
}


/// Returns RandomAccessRange of lazy-demarshalled tuples.
/// Customazable with Demarshaller template.
/// This version does not require RowDescription, but cannot validate row that good.
auto blockToTuples
    (FieldSpec[] spec, alias Demarshaller = DefaultFieldMarshaller)
    (Message[] data)
{
    alias ResTuple = TupleForSpec!(spec, Demarshaller);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);

    //import std.stdio;

    static ResTuple demarshalRow(const(ubyte)[] from)
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
            FormatCode fcode = FCodeOfFSpec!(Demarshaller).F!(colSpec);
            res[i] = Demarshaller!(colSpec).demarshal(vbuf, fcode, len);
            from = from[max(4, len + 4) .. $];
        }
        enforce!PsqlClientException(from.length == 0,
            "%d bytes left in supposedly emptied row".format(from.length));
        return res;
    }

    static struct RowsRange
    {
    private:
        Message[] dataRows;
    public:
        @property size_t length() { return dataRows.length; }
        @property bool empty() { return dataRows.empty; }
        @property ResTuple front()
        {
            return demarshalRow(dataRows[0].data);
        }
        @property ResTuple back()
        {
            return demarshalRow(dataRows[$-1].data);
        }
        ResTuple opIndex(size_t i)
        {
            return demarshalRow(dataRows[i].data);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows);
        }
    }

    return RowsRange(data);
}
