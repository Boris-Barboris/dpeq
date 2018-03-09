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

Every postSimpleQuery or PSQLConnection.sync should be accompanied by getQueryResults
call. */
void postSimpleQuery(ConnT)(ConnT conn, string query)
{
    conn.putQueryMessage(query);
}


/// Pre-parsed sql query with variable parameters.
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

    /// name of this prepared statement, as seen by backend.
    final @property string preparedName() const { return parsedName; }

    final @property short paramCount() const { return m_paramCount; }

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
        const(ObjectID)[] paramTypes = null)
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
    final void postParseMessage()
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
    final void postCloseMessage()
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

    /// bind empty, parameterless portal. resCodes are requested format codes
    /// of resulting columns, keep it null to request everything in text format.
    final void bind(FormatCode[] resCodes = null)
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
    representation of 'args' parameters, marshalled to 'specs' types according
    to 'Marshaller' template. Format codes of the response columns is set
    via 'resCodes' array, known at compile time.
    */
    final void bind(
            FieldSpec[] specs,
            FormatCode[] resCodes = null,
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

    /** This version of bind accept generic InputRanges of format codes and
    field marshallers and passes them directly to putBindMessage method of
    connection object. No parameter count and type validation is performed.
    If this portal is already bound and is a named one, Close message is
    posted.
    */
    final void bind(FR, PR, RR)(scope FR paramCodeRange, scope PR paramMarshRange,
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

    /// Simple portal bind, wich binds all parameters as strings and requests
    /// all result columns in text format.
    final void bind(scope Nullable!(string)[] args)
    {
        assert(prepStmt.paramCount == args.length);

        if (bindRequested && portalName.length)
            postCloseMessage();

        static struct StrMarshaller
        {
            Nullable!string str;
            this(Nullable!string v) { str = v; }

            int opCall(ubyte[] buf)
            {
                return marshalNullableStringField(buf, str);
            }
        }

        static struct MarshRange
        {
            Nullable!(string)[] params;
            int idx = 0;
            @property bool empty() { return idx >= params.length; }
            void popFront() { idx++; }
            @property StrMarshaller front()
            {
                return StrMarshaller(params[idx]);
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
    final void postCloseMessage()
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
    from PSQL - useful for optimistic statically-typed querying. */
    final void execute(bool describe = true)
    {
        assert(bindRequested, "Portal was never bound");
        if (describe)
            conn.putDescribeMessage(StmtOrPortal.Portal, portalName);
        conn.putExecuteMessage(portalName);
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
    bool newBlockAwaited = true;

    bool interceptor(Message msg, ref bool err, ref string errMsg) nothrow
    {
        with (BackendMessageType)
        switch (msg.type)
        {
            case EmptyQueryResponse:
                if (newBlockAwaited)
                {
                    RowBlock rb;
                    rb.emptyQuery = true;
                    res.blocks ~= rb;
                }
                res.commandsComplete++;
                newBlockAwaited = true;
                break;
            case CommandComplete:
                if (newBlockAwaited)
                    res.blocks ~= RowBlock();
                res.commandsComplete++;
                newBlockAwaited = true;
                break;
            case PortalSuspended:
                res.commandsComplete++;
                newBlockAwaited = true;
                res.blocks[$-1].suspended = true;
                break;
            case RowDescription:
                // RowDescription always precedes new row block data
                if (newBlockAwaited)
                {
                    RowBlock rb;
                    rb.rowDesc = dpeq.schema.RowDescription(msg.data);
                    res.blocks ~= rb;
                    newBlockAwaited = false;
                }
                else
                {
                    err = true;
                    errMsg = "Unexpected RowDescription in the middle of " ~
                        "row block";
                }
                break;
            case DataRow:
                if (newBlockAwaited)
                {
                    if (requireRowDescription)
                    {
                        err = true;
                        errMsg ~= "Got row without row description. ";
                    }
                    res.blocks ~= RowBlock();
                    newBlockAwaited = false;
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
                result.emptyQuery = true;
                return true;
            case CommandComplete:
                return true;
            case PortalSuspended:
                result.suspended = true;
                return true;
            case RowDescription:
                result.rowDesc = dpeq.schema.RowDescription(msg.data);
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
                        return true;
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

/** Returns RandomAccessRange of InputRanges of lazy-demarshalled variants.
Specific flavor of Variant is derived from Converter.demarshal call return type.
Look into marshalling.VariantConverter for demarshal implementation examples.
Will append parsed field descriptions to fieldDescs array if passed. */
auto blockToVariants(alias Converter = VariantConverter!DefaultFieldMarshaller)
    (RowBlock block, FieldDescription[]* fieldDescs = null)
{
    alias VariantT = ReturnType!(Converter.demarshal);

    enforce!PsqlMarshallingException(block.rowDesc.isSet,
        "Cannot demarshal RowBlock without row description. " ~
        "Did you send Describe message?");
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

/// Template function Func returns D type wich corresponds to FieldSpec.
template SpecMapper(alias Demarshaller)
{
    template Func(FieldSpec spec)
    {
        static if (is(Demarshaller!spec.type))
            alias Func = Demarshaller!spec.type;
        else
            static assert(0, "Demarshaller doesn't support type with oid " ~
                spec.typeId.to!string);
    }
}


/** Returns RandomAccessRange of lazily-demarshalled tuples.
Customazable with Demarshaller template. Will append parsed field descriptions
to fieldDescs array if it is provided. */
auto blockToTuples
    (FieldSpec[] spec, alias Demarshaller = DefaultFieldMarshaller)
    (RowBlock block, FieldDescription[]* fieldDescs = null)
{
    alias ResTuple = TupleForSpec!(spec, Demarshaller);
    debug pragma(msg, "Resulting tuple from spec: ", ResTuple);
    enforce!PsqlMarshallingException(block.rowDesc.isSet,
        "Cannot demarshal RowBlock without row description. " ~
        "Did you send describe message?");
    short totalColumns = block.rowDesc.fieldCount;
    enforce!PsqlMarshallingException(totalColumns == spec.length,
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
        enforce!PsqlMarshallingException(colType == spec[i].typeId,
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
        from = from[2 .. $];    // skip 16 bits
        foreach (i, colSpec; aliasSeqOf!(spec))
        {
            len = demarshalNumber(from[0 .. 4]);
            //writeln("col ", i, ", len = ", len, " from = ", from);
            vbuf = from[4 .. max(4, len + 4)];
            res[i] = Demarshaller!(colSpec).demarshal(vbuf, fcodes[i], len);
            from = from[max(4, len + 4) .. $];
        }
        enforce!PsqlMarshallingException(from.length == 0,
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


/** Returns RandomAccessRange of lazy-demarshalled tuples. Customazable with
Demarshaller template. This version does not require RowDescription, but cannot
validate row types reliably. */
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
        enforce!PsqlMarshallingException(from.length == 0,
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
