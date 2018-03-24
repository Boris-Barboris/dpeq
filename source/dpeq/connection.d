/**
Connection.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.connection;

import core.time: seconds, Duration;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.range;

import dpeq.constants;
import dpeq.exceptions;
import dpeq.serialize;
import dpeq.result;
import dpeq.socket;



/// Transport and authorization parameters.
struct BackendParams
{
    /// Hostname or IP address for TCP socket. Filename for UNIX socket.
    string host = "/var/run/postgresql/.s.PGSQL.5432";
    ushort port = cast(ushort)5432;
    string user = "postgres";
    string password;
    string database = "postgres";
}

enum StmtOrPortal: char
{
    Statement = 'S',
    Portal = 'P'
}

// When the client code is uninterested in dpeq connection logging.
pragma(inline, true) void nop_logger(T...)(string fmt, T vals) nothrow @safe pure {}

/**
Connection object.

Params:
    SocketT  = socket class type to use.
    logTrace = alias of a logging function with $(D std.stdio.writef) signature.
        Very verbose byte-stream information will be printed through it.
    logError = same as logTrace but for errors.
*/
class PSQLConnection(
    SocketT = StdSocket,
    alias logTrace = nop_logger,
    alias logError = nop_logger)
{
    protected
    {
        SocketT m_socket;
        ubyte[] writeBuffer;
        int bufHead = 0;
        bool open = false;

        // we allocate RAM for responses in batches to reduce GC pressure.
        // 2048 is the last small-sized dling in gc:
        // https://github.com/dlang/druntime/blob/v2.079.0/src/gc/impl/conservative/gc.d#L1251
        immutable int readBatchSize = 2048;
        ubyte[] readBatch;

        // number of expected readyForQuery responses
        int readyForQueryExpected = 0;
        int unflushedRfq = 0;

        TransactionStatus tstatus;

        /// counters to generate unique prepared statement and portal ids
        int preparedCounter = 0;
        int portalCounter = 0;

        int m_processId = -1;
        int m_cancellationKey = -1;

        /// backend params this connection was created with
        const BackendParams m_backendParams;
    }

    /// Backend parameters this connection was constructed from
    final @property ref const(BackendParams) backendParams() const pure @safe
    {
        return m_backendParams;
    }

    /// Backend process ID. Used in CancelRequest message.
    final @property int processId() const pure @safe { return m_processId; }

    /// Cancellation secret. Used in CancelRequest message.
    final @property int cancellationKey() const pure @safe { return m_cancellationKey; }

    /// Socket getter.
    final @property SocketT socket() pure @safe { return m_socket; }

    /// Number of ReadyForQuery messages that are yet to be received
    /// from the backend. May be useful for checking wether getQueryResults
    /// would block forever.
    final @property int expectedRFQCount() const pure @safe { return readyForQueryExpected; }

    /// Transaction status, reported by the last received ReadyForQuery message.
    /// For a new connection TransactionStatus.IDLE is returned.
    final @property TransactionStatus transactionStatus() const pure @safe { return tstatus; }

    invariant
    {
        assert(readyForQueryExpected >= 0);
        assert(unflushedRfq >= 0);
        assert(bufHead >= 0);
    }

    /// Connection is open when it is authorized and socket was alive last time
    /// it was checked.
    final @property bool isOpen() const pure @safe { return open; }

    /// Generate next connection-unique prepared statement name.
    final string getNewPreparedName() pure @safe
    {
        return (preparedCounter++).to!string;
    }

    /// Generate next connection-unique portal name.
    final string getNewPortalName() pure @safe
    {
        return (portalCounter++).to!string;
    }

    /// Allocate reuired memory, build socket and authorize the connection.
    /// Throws: $(D PsqlSocketException) if transport failed, or
    /// $(D PsqlClientException) if authorization failed for some reason.
    this(const BackendParams bp, Duration connectTimeout = seconds(10), size_t writeBufSize = 2 * 4096) @safe
    {
        m_backendParams = bp;
        writeBuffer.length = writeBufSize;
        try
        {
            logTrace("Trying to open TCP connection to PSQL");
            m_socket = new SocketT(bp.host, bp.port, connectTimeout);
            logTrace("Success");
        }
        catch (Exception e)
        {
            throw new PsqlSocketException(e.msg, e);
        }
        scope(failure) m_socket.close();
        initialize(bp);
        open = true;
    }

    /// Notify backend and close socket.
    void terminate() nothrow @safe
    {
        open = false;
        try
        {
            putTerminateMessage();
            flush();
            m_socket.close();
        }
        catch (Exception e)
        {
            logError("Exception caught while terminating PSQL connection: %s", e.msg);
        }
    }

    /**
    Open new socket and connect it to the same backend as this connection is
    bound to, send CancelRequest and close the temporary socket.
    */
    void cancelRequest() @safe
    {
        SocketT sock = new SocketT(m_backendParams.host, m_backendParams.port, seconds(5));
        ubyte[4 * 4] intBuf;
        serializeFixedField(intBuf[0..4], int(16));
        serializeFixedField(intBuf[4..8], int(80877102));
        serializeFixedField(intBuf[8..12], m_processId);
        serializeFixedField(intBuf[12..16], m_cancellationKey);
        sock.send(intBuf[]);
        sock.close();
    }

    /// Flush writeBuffer into the socket. Blocks/yields (according to socket
    /// implementation).
    final void flush() @safe
    {
        try
        {
            // does not block if zero length:
            // https://github.com/vibe-d/vibe-core/blob/master/source/vibe/core/net.d#L607
            auto w = m_socket.send(writeBuffer[0..bufHead]);
            while (bufHead - w > 0)
                w += m_socket.send(writeBuffer[w..bufHead]);
            logTrace("flushed %d bytes: %s", w, writeBuffer[0..bufHead].to!string);
        }
        catch (PsqlSocketException e)
        {
            open = false;
            throw e;
        }
        finally
        {
            bufHead = 0;
            readyForQueryExpected += unflushedRfq;
            unflushedRfq = 0;
        }
    }

    /// discard write buffer content
    final void discard() pure @safe
    {
        bufHead = 0;
        unflushedRfq = 0;
    }

    /// Save write buffer cursor in order to be able to restore it in case of errors.
    /// Use it to prevent sending junk to backend when something goes wrong during
    /// deserialization or message creation.
    final auto saveBuffer() pure @safe
    {
        static struct WriteCursor
        {
            private int savedHead;
            private int savedUnflushedRfq;
            PSQLConnection conn;
            void restore() @safe
            {
                assert(conn.bufHead >= savedHead);
                conn.bufHead = savedHead;
                assert(conn.unflushedRfq >= savedUnflushedRfq);
                conn.unflushedRfq = savedUnflushedRfq;
            }
        }
        return WriteCursor(bufHead, unflushedRfq, this);
    }

    pragma(inline)
    final void ensureOpen() const pure @safe
    {
        enforce!PsqlConnectionClosedException(open, "Connection is not open");
    }


    /*
    ////////////////////////////////////////////////////////////////////////////
    // All sorts of messages
    // https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html
    ////////////////////////////////////////////////////////////////////////////
    */

    /** Put Bind message into write buffer.
    *
    * 'formatCodes' - input range of FormatCodes.
    * quotes:
    * The number of parameter format codes that follow (denoted C below).
    * This can be zero to indicate that there are no parameters or that the
    * parameters all use the default format (text); or one, in which case the
    * specified format code is applied to all parameters; or it can equal
    * the actual number of parameters.
    * The parameter format codes. Each must presently be zero (text) or one (binary).
    * `parameters` is input range of deserialization delegates.
    *
    * 'parameters' - input range of serializeler closures. Actual data should
    * be self-contained in this parameter. Marshaller is a callable that
    * is covariant with "int delegate(ubyte[] buf)" and returns -2 if buf
    * is too small, -1 if parameter is null and an actual number of bytes written
    * otherwise.
    *
    * 'resultFormatCodes' - input range of query result FormatCodes.
    * quotes:
    *
    * The number of result-column format codes that follow (denoted R below).
    * This can be zero to indicate that there are no result columns or that
    * the result columns should all use the default format (text); or one,
    * in which case the specified format code is applied to all result
    * columns (if any); or it can equal the actual number of result columns
    * of the query.
    * The result-column format codes. Each must presently be zero (text) or
    * one (binary).
    */
    final void putBindMessage(FR, PR, RR)
        (string portal, string prepared, scope FR formatCodes, scope PR parameters,
        scope RR resultFormatCodes) pure @safe
    //if (isInputRange!FR && is(Unqual!(ElementType!FR) == FormatCode) &&
    //    isInputRange!RR && is(Unqual!(ElementType!RR) == FormatCode) &&
    //    isInputRange!PR && __traits(compiles, -1 == parameters.front()(new ubyte[2]))
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Bind);
        auto lenTotal = reserveLen();
        cwrite(portal);
        cwrite(prepared);

        // parameter format code(s)
        short fcodes = 0;
        auto fcodePrefix = reserveLen!short();
        foreach (FormatCode fcode; formatCodes)
        {
            logTrace("Bind: writing %d fcode", fcode);
            write(cast(short)fcode);
            fcodes++;
        }
        fcodePrefix.write(fcodes);

        // parameters
        short pcount = 0;
        auto pcountPrefix = reserveLen!short();
        foreach (param; parameters)
        {
            auto paramPrefix = reserveLen!int();
            int r = wrappedSerialize(param);
            logTrace("Bind: wrote 4bytes + %d bytes for value", r);
            paramPrefix.write(r);    // careful! -1 means Null
            pcount++;
        }
        pcountPrefix.write(pcount);

        // result format codes
        short rcount = 0;
        auto rcolPrefix = reserveLen!short();
        foreach (FormatCode fcode; resultFormatCodes)
        {
            write(cast(short)fcode);
            logTrace("Bind: writing %d rfcode", fcode);
            rcount++;
        }
        rcolPrefix.write(rcount);

        lenTotal.fill();
        logTrace("Bind message buffered");
    }

    /// putBindMessage overload for parameterless portals
    final void putBindMessage(RR)
        (string portal, string prepared, scope RR resultFormatCodes) pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Bind);
        auto lenTotal = reserveLen();
        cwrite(portal);
        cwrite(prepared);

        // parameter format code(s)
        short fcodes = 0;
        auto fcodePrefix = reserveLen!short();
        fcodePrefix.write(fcodes);

        // parameters
        short pcount = 0;
        auto pcountPrefix = reserveLen!short();
        pcountPrefix.write(pcount);

        // result format codes
        short rcount = 0;
        auto rcolPrefix = reserveLen!short();
        foreach (FormatCode fcode; resultFormatCodes)
        {
            write(cast(short)fcode);
            logTrace("Bind: writing %d rfcode", fcode);
            rcount++;
        }
        rcolPrefix.write(rcount);

        lenTotal.fill();
        logTrace("Bind message buffered");
    }

    /// put Close message into write buffer.
    /// `closeWhat` is 'S' for prepared statement and
    /// 'P' for portal.
    final void putCloseMessage(StmtOrPortal closeWhat, string name) pure @safe
    {
        ensureOpen();
        assert(closeWhat == 'S' || closeWhat == 'P');
        write(cast(ubyte)FrontMessageType.Close);
        auto lenTotal = reserveLen();
        write(cast(ubyte)closeWhat);
        cwrite(name);
        lenTotal.fill();
        logTrace("Close message buffered");
    }

    /// put Close message into write buffer.
    /// `closeWhat` is 'S' for prepared statement and
    /// 'P' for portal.
    final void putDescribeMessage(StmtOrPortal descWhat, string name) pure @safe
    {
        ensureOpen();
        assert(descWhat == 'S' || descWhat == 'P');
        write(cast(ubyte)FrontMessageType.Describe);
        auto lenTotal = reserveLen();
        write(cast(ubyte)descWhat);
        cwrite(name);
        lenTotal.fill();
        logTrace("Describe message buffered");
    }

    /**
    non-zero maxRows will generate PortalSuspended messages, wich are
    currently not handled by dpeq commands */
    final void putExecuteMessage(string portal = "", int maxRows = 0) pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Execute);
        auto lenTotal = reserveLen();
        cwrite(portal);
        write(maxRows);
        lenTotal.fill();
        logTrace("Execute message buffered");
    }

    /**
    Quote:
    "The Flush message does not cause any specific output to be generated,
    but forces the backend to deliver any data pending in its output buffers.
    A Flush must be sent after any extended-query command except Sync, if the
    frontend wishes to examine the results of that command before issuing more
    commands. Without Flush, messages returned by the backend will be combined
    into the minimum possible number of packets to minimize network overhead."
    */
    final void putFlushMessage() pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Flush);
        write(4);
        logTrace("Flush message buffered");
    }

    final void putParseMessage(PR)(string prepared, string query, scope PR ptypes) pure @safe
        if (isInputRange!PR && is(Unqual!(ElementType!PR) == OID))
    {
        logTrace("Message to parse query: %s", query);

        ensureOpen();
        write(cast(ubyte)FrontMessageType.Parse);
        auto lenTotal = reserveLen();
        cwrite(prepared);
        cwrite(query);

        // parameter types
        short pcount = 0;
        auto pcountPrefix = reserveLen!short();
        foreach (OID ptype; ptypes)
        {
            write(ptype);
            pcount++;
        }
        pcountPrefix.write(pcount);

        lenTotal.fill();
        logTrace("Parse message buffered");
    }

    /// put Query message (simple query protocol) into the write buffer
    final void putQueryMessage(string query) pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Query);
        auto lenTotal = reserveLen();
        cwrite(query);
        lenTotal.fill();
        unflushedRfq++;
        logTrace("Query message buffered");
    }

    alias putSimpleQuery = putQueryMessage;

    /**
    Put Sync message into write buffer. Usually you should call this after
    every portal execute message.
    Every postSimpleQuery or PSQLConnection.sync MUST be accompanied by getQueryResults call. */
    final void putSyncMessage() pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Sync);
        write(4);
        unflushedRfq++;
        logTrace("Sync message buffered");
    }

    alias sync = putSyncMessage;

    /// NotificationResponse messages will be parsed and passed to this
    /// callback during 'pollMessages' call.
    /// https://www.postgresql.org/docs/9.5/static/sql-notify.html
    bool delegate(typeof(this) con, Notification n) nothrow @safe notificationCallback = null;

    /// NoticeResponse messages will be parsed and
    /// passed to this callback during 'pollMessages' call.
    void delegate(typeof(this) con, Notice n) nothrow @safe noticeCallback = null;

    /** When this callback returns true, pollMessages will exit it's loop.
    Interceptor should set err to true if it has encountered some kind of error
    and wants it to be rethrown as PsqlClientException at the end of
    pollMessages call. errMsg should be appended with error description. */
    alias InterceptorT = bool delegate(Message msg, ref bool err,
        ref string errMsg) @safe nothrow;

    /** Read messages from the socket in loop until:
      1). if finishOnError is set and ErrorResponse is received, function
          throws immediately.
      2). ReadyForQuery message is received.
      3). interceptor delegate returns `true`.
      4). NotificationResponse received and notificationCallback returned 'true'.
    Interceptor delegate is used to customize the logic. If the message is
    not ReadyForQuery, ErrorResponse or Notice\Notification, it is passed to
    interceptor. It may set bool 'err' flag to true and append to 'errMsg'
    string, if delayed throw is required. */
    final void pollMessages(scope InterceptorT interceptor, bool finishOnError = false) @safe
    {
        bool finish = false;
        bool error = false;
        Notice errorNotice;
        bool intError = false;
        string intErrMsg;

        while (!finish)
        {
            Message msg = readOneMessage();

            with (BackendMessageType)
            switch (msg.type)
            {
                case ErrorResponse:
                    enforce!PsqlClientException(!error, "Second ErrorResponse " ~
                        "received during one pollMessages call");
                    error = true;
                    parseNoticeMessage(msg.data, errorNotice);
                    if (finishOnError)
                        finish = true;
                    break;
                case ReadyForQuery:
                    enforce!PsqlClientException(readyForQueryExpected > 0,
                        "Unexpected ReadyForQuery message");
                    readyForQueryExpected--;
                    tstatus = cast(TransactionStatus) msg.data[0];
                    finish = true;
                    break;
                case NoticeResponse:
                    if (noticeCallback !is null)
                    {
                        Notice n;
                        parseNoticeMessage(msg.data, n);
                        noticeCallback(this, n);
                    }
                    break;
                case NotificationResponse:
                    if (notificationCallback !is null)
                    {
                        Notification n;
                        n.procId = deserializeNumber!int(msg.data[0..4]);
                        size_t l;
                        n.channel = deserializeProtocolString(msg.data[4..$], l);
                        n.payload = deserializeString(msg.data[4+l..$-1]);
                        finish |= notificationCallback(this, n);
                    }
                    break;
                default:
                    if (interceptor !is null)
                        finish |= interceptor(msg, intError, intErrMsg);
            }
        }

        if (error)
            throw new PsqlErrorResponseException(errorNotice);
        if (intError)
            throw new PsqlClientException(intErrMsg);
    }

    /// reads and discards messages from socket until all expected
    /// ReadyForQuery messages are received
    void windupResponseStack() @safe
    {
        while (readyForQueryExpected > 0)
        {
            Message msg = readOneMessage();
            if (msg.type == BackendMessageType.ReadyForQuery)
                readyForQueryExpected--;
        }
    }

    // Protected section for functions that will probably never be used by
    // client code directly. If you need them, inherit them.
protected:

    final void putStartupMessage(in BackendParams params) pure @safe
    {
        auto lenPrefix = reserveLen();
        write(0x0003_0000);  // protocol v3
        cwrite("user");
        cwrite(params.user);
        cwrite("database");
        cwrite(params.database);
        write(cast(ubyte)0);
        lenPrefix.fill();
        logTrace("Startup message buffered");
    }

    final void putTerminateMessage() pure @safe
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Terminate);
        write(4);
        logTrace("Terminate message buffered");
    }

    void initialize(in BackendParams params) @safe
    {
        putStartupMessage(params);
        flush();

        int authType = -1;
        Message auth_msg;

        pollMessages((Message msg, ref bool e, ref string eMsg) {
                if (msg.type == BackendMessageType.Authentication)
                {
                    auth_msg = msg;
                    if (authType != -1)
                    {
                        e = true;
                        eMsg ~= "Unexpected second Authentication " ~
                            "message received from backend";
                    }
                    authType = deserializeNumber(msg.data[0..4]);
                    if (authType == 0)  // instantly authorized, so we'll get readyForQuery
                        readyForQueryExpected++;
                    else
                        return true;
                }
                else if (msg.type == BackendMessageType.BackendKeyData)
                {
                    m_processId = deserializeNumber(msg.data[0..4]);
                    m_cancellationKey = deserializeNumber(msg.data[4..8]);
                }
                return false;
            }, true);

        enforce!PsqlClientException(authType != -1,
            "Expected Authentication message was not received");
        switch (authType)
        {
            case 0:
                // instant AuthenticationOk, trusted connection usually does this
                logTrace("Succesfully authorized");
                return;
            case 3:
                // cleartext password
                putPasswordMessage(params.password);
                break;
            case 5:
                // MD5 salted password
                ubyte[4] salt = auth_msg.data[4 .. 8];
                putMd5PasswordMessage(params.password, params.user, salt);
                break;
            default:
                throw new PsqlClientException("Unknown auth type " ~
                    authType.to!string);
        }
        flush();

        int authRes = -1;
        pollMessages((Message msg, ref bool e, ref string eMsg) {
                if (msg.type == BackendMessageType.Authentication)
                    authRes = deserializeNumber(msg.data[0..4]);
                else if (msg.type == BackendMessageType.BackendKeyData)
                {
                    m_processId = deserializeNumber(msg.data[0..4]);
                    m_cancellationKey = deserializeNumber(msg.data[4..8]);
                }
                return false;
            });
        enforce!PsqlClientException(authRes == 0,
            "Expected AuthenticationOk message was not received");
    }

    final void putPasswordMessage(string pw) pure @safe
    {
        write(cast(ubyte)FrontMessageType.PasswordMessage);
        auto lenPrefix = reserveLen();
        cwrite(pw);
        lenPrefix.fill();
        unflushedRfq++;
        logTrace("Password message buffered");
    }

    final void putMd5PasswordMessage(string pw, string user, ubyte[4] salt) pure @trusted
    {
        // thank you ddb authors
        char[32] MD5toHex(T...)(in T data)
        {
            import std.ascii : LetterCase;
            import std.digest.md : md5Of, toHexString;
            return md5Of(data).toHexString!(LetterCase.lower);
        }

        write(cast(ubyte)FrontMessageType.PasswordMessage);
        auto lenPrefix = reserveLen();
        char[3 + 32] mdpw;
        mdpw[0 .. 3] = "md5";
        mdpw[3 .. $] = MD5toHex(MD5toHex(pw, user), salt);
        cwrite(cast(string) mdpw[]);
        lenPrefix.fill();
        unflushedRfq++;
        logTrace("MD5 Password message buffered");
    }

    /// Read from socket to buffer buf exactly buf.length bytes.
    /// Blocks and throws.
    final void read(ubyte[] buf) @safe
    {
        try
        {
            logTrace("reading %d bytes", buf.length);
            auto r = m_socket.receive(buf);
            assert(r == buf.length, "received less bytes than requested");
        }
        catch (PsqlSocketException e)
        {
            open = false;
            throw e;
        }
    }

    /// extends writeBuffer if serializer 'm' is lacking space (returns -2)
    final int wrappedSerialize(SerialT)(scope SerialT m) pure @safe
        if (isCallable!SerialT)
    {
        int bcount = m(writeBuffer[bufHead .. $]);
        while (bcount == -2)
        {
            // reallocate with additional 4 pages
            writeBuffer.length = writeBuffer.length + 4 * 4096;
            bcount = m(writeBuffer[bufHead .. $]);
        }
        if (bcount > 0)
            bufHead += bcount;
        return bcount;
    }

    /// write numeric type T to write buffer
    final int write(T)(T val) pure @safe nothrow
        if (isNumeric!T)
    {
        return wrappedSerialize((ubyte[] buf) => serializeFixedField(buf, val));
    }

    /// Reserve space in write buffer for length prefix and return
    /// struct that automatically fills it from current buffer offset position.
    final auto reserveLen(T = int)() @safe nothrow
        if (isNumeric!T && !isUnsigned!T)
    {
        static struct Len
        {
            PSQLConnection con;
            int idx;    // offset of length prefix word in writeBuffer

            /// calculate and write length prefix
            void fill(bool includeSelf = true) pure @safe
            {
                T len = (con.bufHead - idx).to!T;
                if (!includeSelf)
                {
                    len -= T.sizeof.to!T;
                    assert(len >= 0);
                }
                else
                    assert(len >= T.sizeof);
                logTrace("writing length of %d bytes to index %d", len, idx);
                auto res = serializeFixedField(con.writeBuffer[idx .. idx+T.sizeof], len);
                assert(res == T.sizeof);
            }

            /// write some specific number
            void write(T v) nothrow pure @safe
            {
                auto res = serializeFixedField(con.writeBuffer[idx .. idx+T.sizeof], v);
                assert(res == T.sizeof);
            }
        }

        Len l = Len(this, bufHead);
        bufHead += T.sizeof;
        return l;
    }

    final int write(string s) pure @safe
    {
        return wrappedSerialize((ubyte[] buf) => serializeStringField(buf, s));
    }

    final int cwrite(string s) pure @safe
    {
        return wrappedSerialize((ubyte[] buf) => serializeCstring(buf, s));
    }

    /// read exactly one message from the socket
    Message readOneMessage() @trusted
    {
        Message res;
        ubyte[5] type_and_length;
        read(type_and_length);
        res.type = cast(BackendMessageType) type_and_length[0];
        logTrace("Got message of type %s", res.type.to!string);
        int length = deserializeNumber(cast(immutable(ubyte)[]) type_and_length[1..$]) - 4;
        enforce!PsqlClientException(length >= 0, "Negative message length");
        ubyte[] data;
        if (length > 0)
        {
            if (length <= readBatchSize / 2)
            {
                // we should batch the allocation
                if (readBatch.length < length)
                    readBatch = new ubyte[readBatchSize];
                data = readBatch[0..length];
                readBatch = readBatch[length..$];
            }
            else
                data = new ubyte[length];   // fat messages get their own buffer
            read(data);
        }
        res.data = cast(immutable(ubyte)[]) data;
        return res;
    }
}
