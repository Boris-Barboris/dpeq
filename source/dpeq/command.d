/**
Functions of various nature. Prepared statement, portal and response deserialization
functions live here.

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
import dpeq.serialize;
import dpeq.result;



@safe:

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

Every postSimpleQuery or PSQLConnection.sync should be accompanied by getQueryResults
call. */
void postSimpleQuery(ConnT)(ConnT conn, string query) pure
{
    conn.putQueryMessage(query);
}

/// Pre-parsed sql query with variable parameters.
class PreparedStatement(ConnT)
{
    protected
    {
        const(OID)[] paramTypes;
        string query;
        ConnT conn;
        string parsedName;  // name, reserved for this statement in PSQL connection
        short m_paramCount;
        bool parseRequested;
    }

    /// name of this prepared statement, as seen by backend.
    final @property string preparedName() const pure { return parsedName; }

    final @property short paramCount() const pure { return m_paramCount; }

    /**
    Creates prepared statement object, wich holds dpeq utility state.
    Constructor does not write anything to connection write buffer.

    Quoting https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html:

    The number of parameter data types specified (can be zero). Note that this is not an
    indication of the number of parameters that might appear in the query string,
    only the number that the frontend wants to prespecify types for.

    Then, for each parameter, there is the following:

    Int32
        Specifies the object ID of the parameter data type. Placing a zero here is
        equivalent to leaving the type unspecified.

    That means you can leave paramTypes null, unless you're doing some tricky
    stuff.
    */
    this(ConnT conn, string query, short paramCount, bool named = false,
        const(OID)[] paramTypes = null) pure
    {
        assert(conn);
        assert(query);
        assert(paramCount >= 0);
        this.conn = conn;
        this.query = query;
        this.paramTypes = paramTypes;
        this.m_paramCount = paramCount;
        if (named)
            parsedName = conn.getNewPreparedName();
        else
            parsedName = "";
    }

    /// write Parse message into connection's write buffer.
    final void postParseMessage() pure
    {
        conn.putParseMessage(parsedName, query, paramTypes[]);
        parseRequested = true;
    }

    /// ditto
    alias parse = postParseMessage;

    /** Post message to destroy named prepared statement.

    An unnamed prepared statement lasts only until the next Parse
    statement specifying the unnamed statement as destination is issued.
    (Note that a simple Query message also destroys the unnamed statement.)
    */
    final void postCloseMessage() pure
    {
        assert(parseRequested, "prepared statement was never sent to backend");
        assert(parsedName.length, "no need to close unnamed prepared statements");
        conn.putCloseMessage(StmtOrPortal.Statement, parsedName);
        parseRequested = false;
    }

    /// poll message queue and make sure parse was completed
    final void ensureParseComplete()
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
        enforce!PsqlClientException(parsed, "Parse was not confirmed");
    }
}


/// Parameter tuple, bound to prepared statement
class Portal(ConnT)
{
    protected
    {
        PreparedStatement!ConnT prepStmt;
        ConnT conn;
        string portalName;  // name, reserved for this portal in PSQL connection
        bool bindRequested = false;
    }

    this(PreparedStatement!ConnT ps, bool persist = true) pure
    {
        assert(ps);
        this.conn = ps.conn;
        prepStmt = ps;
        if (persist)
            portalName = conn.getNewPortalName();
        else
            portalName = "";
    }

    /// bind empty, parameterless portal. resCodes are requested format codes
    /// of resulting columns, keep it null to request everything in text format.
    final void bind(FormatCode[] resCodes = null) pure
    {
        assert(prepStmt.paramCount == 0);

        auto safePoint = conn.saveBuffer();
        scope (failure) safePoint.restore();

        if (bindRequested && portalName.length)
            postCloseMessage();

        conn.putBindMessage(portalName, prepStmt.parsedName, resCodes);
        bindRequested = true;
    }

    /**
    For the 'specs' array of prepared statement parameters types, known at
    compile-time, write Bind message to connection's write buffer from the
    representation of 'args' parameters, serialized to 'specs' types according
    to 'Serializer' template. Format codes of the response columns is set
    via 'resCodes' array, known at compile time.
    */
    final void bind(
            FieldSpec[] specs,
            FormatCode[] resCodes = null,
            alias Serializer = DefaultSerializer,
            Args...)
        (in Args args) pure
    {
        assert(prepStmt.paramCount == Args.length);
        assert(prepStmt.paramCount == specs.length);

        auto safePoint = conn.saveBuffer();
        scope (failure) safePoint.restore();

        if (bindRequested && portalName.length)
            postCloseMessage();

        enum fcodesr = [staticMap!(FCodeOfFSpec!(Serializer).F, aliasSeqOf!specs)];

        alias DlgT = int delegate(ubyte[]) pure @safe;
        DlgT[specs.length] serializers;
        foreach(i, paramSpec; aliasSeqOf!specs)
        {
            serializers[i] =
                (ubyte[] to) => Serializer!paramSpec.serialize(to, args[i]);
        }
        conn.putBindMessage(portalName, prepStmt.parsedName, fcodesr,
            serializers, resCodes);
        bindRequested = true;
    }

    /** This version of bind accept generic InputRanges of format codes and
    field serializers and passes them directly to putBindMessage method of
    connection object. No parameter count and type validation is performed.
    If this portal is already bound and is a named one, Close message is
    posted.
    */
    final void bind(FR, PR, RR)(scope FR paramCodeRange, scope PR paramRange,
        scope RR returnCodeRange) pure
    {
        auto safePoint = conn.saveBuffer();
        scope (failure) safePoint.restore();
        if (bindRequested && portalName.length)
            postCloseMessage();
        conn.putBindMessage(portalName, prepStmt.parsedName, paramCodeRange,
            paramRange, returnCodeRange);
        bindRequested = true;
    }

    /// Simple portal bind, wich binds all parameters as strings and requests
    /// all result columns in text format.
    final void bind(scope const(Nullable!string)[] args) pure
    {
        assert(prepStmt.paramCount == args.length);

        if (bindRequested && portalName.length)
            postCloseMessage();

        static struct StrSerializer
        {
            const Nullable!string str;
            this(const(Nullable!string) v) { str = v; }

            int opCall(ubyte[] buf) const
            {
                return serializeNullableStringField(buf, str);
            }
        }

        static struct MarshRange
        {
            const(Nullable!string)[] params;
            int idx = 0;
            @property bool empty() const { return idx >= params.length; }
            void popFront() { idx++; }
            @property StrSerializer front() const
            {
                return StrSerializer(params[idx]);
            }
        }

        conn.putBindMessage!(FormatCode[], MarshRange, FormatCode[])(
            portalName, prepStmt.parsedName, null, MarshRange(args), null);
        bindRequested = true;
    }

    /** Write Close message to connection write buffer in order to
    explicitly destroy named portal.

    If successfully created, a named portal object lasts till the end of the
    current transaction, unless explicitly destroyed. An unnamed portal is
    destroyed at the end of the transaction, or as soon as the next Bind
    statement specifying the unnamed portal as destination is issued.
    (Note that a simple Query message also destroys the unnamed portal.)
    Named portals must be explicitly closed before they can be redefined
    by another Bind message, but this is not required for the unnamed portal.
    */
    final void postCloseMessage() pure
    {
        assert(bindRequested, "portal was never bound");
        assert(portalName.length, "no need to close unnamed portals");
        conn.putCloseMessage(StmtOrPortal.Portal, portalName);
        bindRequested = false;
    }

    /// poll message queue and make sure bind was completed
    final void ensureBindComplete()
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
        enforce!PsqlClientException(is_bound, "Bind was not confirmed");
    }

    /** Send Describe+Execute command.
    If describe is false, no RowDescription message will be requested
    from PSQL - useful for optimistic statically-typed querying.
    'maxRows' parameter is responsible for portal suspending and is
    conceptually inferior to simple TCP backpressure mechanisms or result set
    size limiting. */
    final void execute(bool describe = true, int maxRows = 0) pure
    {
        assert(bindRequested, "Portal was never bound");
        if (describe)
            conn.putDescribeMessage(StmtOrPortal.Portal, portalName);
        conn.putExecuteMessage(portalName, maxRows);
    }
}


/*
////////////////////////////////////////
// Functions to work with query results
////////////////////////////////////////
*/


/** Generic result materializer, suitable for both simple and prepared queries.
Polls messages from the connection and builds QueryResult structure from
them. Throws if something goes wrong. Polling stops when ReadyForQuery message
is received. */
QueryResult getQueryResults(ConnT)(ConnT conn, bool requireRowDescription = false)
{
    QueryResult res;
    RowBlock rb;

    bool interceptor(Message msg, ref bool err, ref string errMsg) nothrow
    {
        with (BackendMessageType)
        switch (msg.type)
        {
            case EmptyQueryResponse:
                rb.state = RowBlockState.emptyQuery;
                res.blocks ~= rb;
                rb = RowBlock();
                break;
            case CommandComplete:
                rb.state = RowBlockState.complete;
                rb.commandTag = deserializeString(msg.data[0..$-1]);
                res.blocks ~= rb;
                rb = RowBlock();
                break;
            case PortalSuspended:
                rb.state = RowBlockState.suspended;
                res.blocks ~= rb;
                rb = RowBlock();
                break;
            case RowDescription:
                // RowDescription always precedes new row block data
                rb.rowDesc = dpeq.result.RowDescription(msg.data);
                break;
            case DataRow:
                if (requireRowDescription)
                {
                    err = true;
                    errMsg ~= "Received row without row description. ";
                }
                rb.dataRows ~= msg;
                break;
            default:
                break;
        }
        return false;
    }

    conn.pollMessages(&interceptor, false);
    return res;
}


/// Poll messages from the connection until CommandComplete or EmptyQueryResponse
/// is received, and return one row block (result of one and only one query).
RowBlock getOneRowBlock(ConnT)(ConnT conn, int rowCountLimit = 0,
    bool requireRowDescription = false)
{
    RowBlock result;

    bool interceptor(Message msg, ref bool err, ref string errMsg) nothrow
    {
        with (BackendMessageType)
        switch (msg.type)
        {
            case EmptyQueryResponse:
                result.state = RowBlockState.emptyQuery;
                return true;
            case CommandComplete:
                result.state = RowBlockState.complete;
                result.commandTag = deserializeString(msg.data[0..$-1]);
                return true;
            case PortalSuspended:
                result.state = RowBlockState.suspended;
                return true;
            case RowDescription:
                result.rowDesc = dpeq.result.RowDescription(msg.data);
                requireRowDescription = false;
                break;
            case DataRow:
                if (requireRowDescription)
                {
                    err = true;
                    errMsg ~= "Missing required RowDescription. ";
                    break;
                }
                result.dataRows ~= msg;
                if (rowCountLimit != 0)
                {
                    // client code requested early stop
                    rowCountLimit--;
                    if (rowCountLimit == 0)
                    {
                        result.state = RowBlockState.incomplete;
                        return true;
                    }
                }
                break;
            default:
                break;
        }
        return false;
    }

    conn.pollMessages(&interceptor, true);
    return result;
}



/*
/////////////////////////////////////////////////////////////////
// Functions used to transform query results into D types
/////////////////////////////////////////////////////////////////
*/

//import std.stdio;

/** Returns RandomAccessRange of InputRanges of lazy-deserialized variants.
Specific flavor of Variant is derived from Converter.deserialize call return type.
Look into serialize.VariantConverter for deserialize implementation examples.
Will append parsed field descriptions to fieldDescs array if passed. */
auto blockToVariants(alias Converter = VariantConverter!DefaultSerializer)
    (RowBlock block, FieldDescription[]* fieldDescs = null) pure
{
    alias VariantT = ReturnType!(Converter.deserialize);

    enforce!PsqlSerializationException(block.rowDesc.isSet,
        "Cannot deserialize RowBlock without row description. " ~
        "Did you send Describe message?");
    short totalColumns = block.rowDesc.fieldCount;
    OID[] typeArr = new OID[totalColumns];
    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description deserialization happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        if (fieldDescs)
            (*fieldDescs)[i] = fdesc;
        fcArr[i] = fdesc.formatCode;
        typeArr[i++] = fdesc.type;
    }

    static struct RowDeserializer
    {
    private:
        short column = 0;
        short totalCols;
        immutable(ubyte)[] buf;
        const(OID)[] types;
        const(FormatCode)[] fcodes;
        bool parsed = false;

        // cache result to prevent repeated deserialization on
        // front() call.
        VariantT res;
    public:
        @property bool empty() const { return column >= totalCols; }
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
            int len = deserializeNumber(buf[0 .. 4]);
            immutable(ubyte)[] vbuf = buf[4 .. max(4, len + 4)];
            //writeln(types[column], " ", buf);
            res = Converter.deserialize(vbuf, types[column], fcodes[column], len);
            buf = buf[max(4, len + 4) .. $];
            parsed = true;
            return res;
        }
    }

    static assert (isInputRange!RowDeserializer);

    static struct RowsRange
    {
    private:
        Message[] dataRows;
        OID[] columnTypes;
        FormatCode[] fcodes;
        short totalColumns;
    public:
        @property size_t length() const { return dataRows.length; }
        @property bool empty() const { return dataRows.empty; }
        @property RowDeserializer front()
        {
            return RowDeserializer(0, totalColumns, dataRows[0].data,
                columnTypes, fcodes);
        }
        @property RowDeserializer back()
        {
            return RowDeserializer(0, totalColumns, dataRows[$-1].data,
                columnTypes, fcodes);
        }
        RowDeserializer opIndex(size_t i)
        {
            return RowDeserializer(0, totalColumns, dataRows[i].data,
                columnTypes, fcodes);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows, columnTypes, fcodes, totalColumns);
        }
    }

    static assert (isRandomAccessRange!RowsRange);

    return RowsRange(block.dataRows, typeArr, fcArr, totalColumns);
}




/// for row spec `spec` build native tuple representation.
template TupleForSpec(FieldSpec[] spec, alias Deserializer = DefaultSerializer)
{
    alias TupleForSpec =
        Tuple!(
            staticMap!(
                SpecMapper!(Deserializer).Func,
                aliasSeqOf!spec));
}

/// Template function Func returns D type wich corresponds to FieldSpec.
template SpecMapper(alias Deserializer)
{
    template Func(FieldSpec spec)
    {
        static if (is(Deserializer!spec.type))
            alias Func = Deserializer!spec.type;
        else
            static assert(0, "Deserializer doesn't support type with oid " ~
                spec.typeId.to!string);
    }
}


/** Returns RandomAccessRange of lazily-deserialized tuples.
Customazable with Deserializer template. Will append parsed field descriptions
to fieldDescs array if it is provided. */
auto blockToTuples
    (FieldSpec[] spec, alias Deserializer = DefaultSerializer)
    (RowBlock block, FieldDescription[]* fieldDescs = null) pure
{
    alias ResTuple = TupleForSpec!(spec, Deserializer);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);
    enforce!PsqlSerializationException(block.rowDesc.isSet,
        "Cannot deserialize RowBlock without row description. " ~
        "Did you send describe message?");
    short totalColumns = block.rowDesc.fieldCount;
    enforce!PsqlSerializationException(totalColumns == spec.length,
        "Expected %d columns in a row, got %d".format(spec.length, totalColumns));
    FormatCode[] fcArr = new FormatCode[totalColumns];

    int i = 0;
    foreach (fdesc; block.rowDesc[]) // row description deserialization happens here
    {
        //writeln(fdesc.name);
        //writeln(fdesc.formatCode);
        if (fieldDescs)
            (*fieldDescs)[i] = fdesc;
        fcArr[i] = fdesc.formatCode;
        OID colType = fdesc.type;
        enforce!PsqlSerializationException(colType == spec[i].typeId,
            "Colunm %d type mismatch: expected %d, got %d".format(
                i, spec[i].typeId, colType));
        i++;
    }

    //import std.stdio;

    static ResTuple deserializeRow(immutable(ubyte)[] from, const(FormatCode)[] fcodes)
    {
        ResTuple res;
        int len = 0;
        immutable(ubyte)[] vbuf;
        from = from[2 .. $];    // skip 16 bits
        foreach (i, colSpec; aliasSeqOf!(spec))
        {
            len = deserializeNumber(from[0 .. 4]);
            //writeln("col ", i, ", len = ", len, " from = ", from);
            vbuf = from[4 .. max(4, len + 4)];
            res[i] = Deserializer!(colSpec).deserialize(vbuf, fcodes[i], len);
            from = from[max(4, len + 4) .. $];
        }
        enforce!PsqlSerializationException(from.length == 0,
            "%d bytes left in supposedly emptied row".format(from.length));
        return res;
    }

    static struct RowsRange
    {
    private:
        Message[] dataRows;
        FormatCode[] fcodes;
    public:
        @property size_t length() const { return dataRows.length; }
        @property bool empty() const { return dataRows.empty; }
        @property ResTuple front()
        {
            return deserializeRow(dataRows[0].data, fcodes);
        }
        @property ResTuple back()
        {
            return deserializeRow(dataRows[$-1].data, fcodes);
        }
        ResTuple opIndex(size_t i)
        {
            return deserializeRow(dataRows[i].data, fcodes);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows, fcodes);
        }
    }

    static assert (isRandomAccessRange!RowsRange);

    return RowsRange(block.dataRows, fcArr);
}


class FormatCodesOfSpec(FieldSpec[] spec, alias Deserializer)
{
    static const(FormatCode)[spec.length] codes;

    static this()
    {
        foreach (i, fpec; aliasSeqOf!spec)
            codes[i] = Deserializer!fpec.formatCode;
    }
}


/** Returns RandomAccessRange of lazy-deserialized tuples. Customazable with
Deserializer template. This version does not require RowDescription, but cannot
validate row types reliably. */
auto blockToTuples
    (FieldSpec[] spec, alias Deserializer = DefaultSerializer)
    (Message[] data) pure
{
    alias ResTuple = TupleForSpec!(spec, Deserializer);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);

    //import std.stdio;

    static ResTuple deserializeRow(immutable(ubyte)[] from)
    {
        ResTuple res;
        int len = 0;
        immutable(ubyte)[] vbuf;
        from = from[2 .. $];    // skip 16 bytes
        foreach (i, colSpec; aliasSeqOf!(spec))
        {
            len = deserializeNumber(from[0 .. 4]);
            //writeln("col ", i, ", len = ", len, " from = ", from);
            vbuf = from[4 .. max(4, len + 4)];
            FormatCode fcode = FCodeOfFSpec!(Deserializer).F!(colSpec);
            res[i] = Deserializer!(colSpec).deserialize(vbuf, fcode, len);
            from = from[max(4, len + 4) .. $];
        }
        enforce!PsqlSerializationException(from.length == 0,
            "%d bytes left in supposedly emptied row".format(from.length));
        return res;
    }

    static struct RowsRange
    {
    private:
        Message[] dataRows;
    public:
        @property size_t length() const { return dataRows.length; }
        @property bool empty() const { return dataRows.empty; }
        @property ResTuple front()
        {
            return deserializeRow(dataRows[0].data);
        }
        @property ResTuple back()
        {
            return deserializeRow(dataRows[$-1].data);
        }
        ResTuple opIndex(size_t i)
        {
            return deserializeRow(dataRows[i].data);
        }
        void popFront() { dataRows = dataRows[1 .. $]; }
        void popBack() { dataRows = dataRows[0 .. $-1]; }
        RowsRange save()
        {
            return RowsRange(dataRows);
        }
    }

    static assert (isRandomAccessRange!RowsRange);

    return RowsRange(data);
}
