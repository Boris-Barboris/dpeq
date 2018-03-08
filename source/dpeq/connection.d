/**
Connection.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.connection;

import core.time: seconds;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.range;
import std.socket;

import dpeq.constants;
import dpeq.exceptions;
import dpeq.marshalling;
import dpeq.schema: Notification, Notice;



/**
$(D std.socket.Socket) wrapper wich is compatible with PSQLConnection.
If you want to use custom socket type (vibe-d or any other), make it
duck-type and exception-compatible with this one.
*/
final class StdSocket
{
    private Socket m_socket;

    /// Underlying socket instance.
    @property Socket socket() { return m_socket; }

    /// Establish connection to backend. Constructor is expected to throw Exception
    /// if anything goes wrong.
    this(string host, ushort port)
    {
        if (host[0] == '/')
        {
            // Unix socket
            version(Posix)
            {
                Address addr = new UnixAddress(host);
                m_socket = new Socket(AddressFamily.UNIX, SocketType.STREAM);
                m_socket.connect(addr);
            }
            else
                assert(0, "Cannot connect using UNIX sockets on non-Posix OS");
        }
        else
        {
            m_socket = new TcpSocket();
            // 20 minutes for both tcp_keepalive_time and tcp_keepalive_intvl
            m_socket.setKeepAlive(1200, 1200);
            // example of receive timeout:
            // m_socket.setOption(SocketOptionLevel.SOCKET, SocketOption.RCVTIMEO, seconds(120));
            m_socket.connect(new InternetAddress(host, port));
        }
    }

    void close() nothrow @nogc
    {
        m_socket.shutdown(SocketShutdown.BOTH);
        m_socket.close();
    }

    /// Send whole byte buffer or throw. Throws: $(D PsqlSocketException).
    auto send(const(ubyte)[] buf)
    {
        auto r = m_socket.send(buf);
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on send");
        return r;
    }

    /// Fill byte buffer from the socket completely or throw.
    /// Throws: $(D PsqlSocketException).
    auto receive(ubyte[] buf)
    {
        auto r = m_socket.receive(buf);
        if (r == 0 && buf.length > 0)
            throw new PsqlSocketException("Connection closed");
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on receive: " ~ m_socket.getErrorText());
        return r;
    }
}



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


/// Message, received from backend.
struct Message
{
    BackendMessageType type;
    /// Raw unprocessed message byte array excluding message type byte and
    /// first 4 bytes that represent message body length.
    ubyte[] data;
}

enum StmtOrPortal: char
{
    Statement = 'S',
    Portal = 'P'
}

// When the client code is uninterested in dpeq connection logging.
private pragma(inline, true) void nop_logger(T...)(T vals) {}

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

        // number of expected readyForQuery responses
        int readyForQueryExpected = 0;

        TransactionStatus tstatus = TransactionStatus.IDLE;

        /// counters to generate unique prepared statement and portal ids
        int preparedCounter = 0;
        int portalCounter = 0;

        int m_processId = -1;
        int m_cancellationKey = -1;

        /// backend params this connection was created with
        const BackendParams m_backendParams;
    }

    /// Backend parameters this connection was constructed from
    final @property ref const(BackendParams) backendParams() const
    {
        return m_backendParams;
    }

    /// Backend process ID. Used in CancelRequest message.
    final @property int processId() const { return m_processId; }

    /// Cancellation secret. Used in CancelRequest message.
    final @property int cancellationKey() const { return m_cancellationKey; }

    /// Socket getter.
    final @property SocketT socket() { return m_socket; }

    /// Number of ReadyForQuery messages that are yet to be received
    /// from the backend. May be useful for checking wether getQueryResults
    /// would block forever.
    final @property int expectedRFQCount() const { return readyForQueryExpected; }

    /// Transaction status, reported by the last received ReadyForQuery message.
    /// For a new connection TransactionStatus.IDLE is returned.
    final @property TransactionStatus transactionStatus() const { return tstatus; }

    invariant
    {
        assert(readyForQueryExpected >= 0);
        assert(bufHead >= 0);
    }

    /// Connection is open when it is authorized and socket was alive last time
    /// it was checked.
    final @property bool isOpen() const { return open; }

    /// Generate next connection-unique prepared statement name.
    final string getNewPreparedName()
    {
        return (preparedCounter++).to!string;
    }

    /// Generate next connection-unique portal name.
    final string getNewPortalName()
    {
        return (portalCounter++).to!string;
    }

    /// Allocate reuired memory, build socket and authorize the connection.
    /// Throws: $(D PsqlSocketException) if transport failed, or
    /// $(D PsqlClientException) if authorization failed for some reason.
    this(const BackendParams bp)
    {
        m_backendParams = bp;
        writeBuffer.length = 2 * 4096; // liberal procurement of two pages
        try
        {
            logTrace("Trying to open TCP connection to PSQL");
            m_socket = new SocketT(bp.host, bp.port);
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
    void terminate()
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
    bound to, send CancelRequest and close socket.
    */
    void cancelRequest()
    {
        SocketT sock = new SocketT(m_backendParams.host, m_backendParams.port);
        ubyte[4 * 4] intBuf;
        marshalFixedField(intBuf[0..4], int(16));
        marshalFixedField(intBuf[4..8], int(80877102));
        marshalFixedField(intBuf[8..12], m_processId);
        marshalFixedField(intBuf[12..16], m_cancellationKey);
        sock.send(intBuf[]);
        sock.close();
    }

    /// Flush writeBuffer into the socket. Blocks/yields (according to socket
    /// implementation).
    final void flush()
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
        }
    }

    /// discard write buffer content
    final void discard()
    {
        bufHead = 0;
    }

    /// Save write buffer cursor in order to be able to restore it in case of errors.
    /// Use it to prevent sending junk to backend when something goes wrong during
    /// marshalling or message creation.
    final auto saveBuffer()
    {
        static struct WriteCursor
        {
            int offset;
            PSQLConnection conn;
            void restore()
            {
                conn.bufHead = offset;
            }
        }
        return WriteCursor(bufHead, this);
    }

    pragma(inline)
    final void ensureOpen()
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
    * `parameters` is input range of marshalling delegates.
    *
    * 'parameters' - input range of marshaller closures. Actual data should
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
        scope RR resultFormatCodes)
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
            int r = wrappedMarsh(param);
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
        (string portal, string prepared, scope RR resultFormatCodes)
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
    final void putCloseMessage(StmtOrPortal closeWhat, string name)
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
    final void putDescribeMessage(StmtOrPortal descWhat, string name)
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
    final void putExecuteMessage(string portal = "", int maxRows = 0)
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
    final void putFlushMessage()
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Flush);
        write(4);
        logTrace("Flush message buffered");
    }

    final void putParseMessage(PR)(string prepared, string query, scope PR ptypes)
        if (isInputRange!PR && is(Unqual!(ElementType!PR) == ObjectID))
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
        foreach (ObjectID ptype; ptypes)
        {
            write(cast(int)ptype);
            pcount++;
        }
        pcountPrefix.write(pcount);

        lenTotal.fill();
        logTrace("Parse message buffered");
    }

    /// put Query message (simple query protocol) into the write buffer
    final void putQueryMessage(string query)
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Query);
        auto lenTotal = reserveLen();
        cwrite(query);
        lenTotal.fill();
        readyForQueryExpected++;
        logTrace("Query message buffered");
    }

    alias putSimpleQuery = putQueryMessage;

    /**
    Put Sync message into write buffer. Usually you should call this after
    every portal execute message.
    Every postSimpleQuery or PSQLConnection.sync MUST be accompanied by getQueryResults call. */
    final void putSyncMessage()
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Sync);
        write(4);
        readyForQueryExpected++;
        logTrace("Sync message buffered");
    }

    alias sync = putSyncMessage;

    /// NotificationResponse messages will be parsed and passed to this
    /// callback during 'pollMessages' call.
    /// https://www.postgresql.org/docs/9.5/static/sql-notify.html
    bool delegate(typeof(this) con, Notification n) nothrow notificationCallback = null;

    /// NoticeResponse messages will be parsed and
    /// passed to this callback during 'pollMessages' call.
    void delegate(typeof(this) con, Notice n) nothrow noticeCallback = null;

    /** When this callback returns true, pollMessages will exit it's loop.
    Interceptor should set err to true if it has encountered some kind of error
    and wants it to be rethrown as PsqlClientException at the end of
    pollMessages call. errMsg should be appended with error description. */
    alias InterceptorT = bool delegate(Message msg, ref bool err,
        ref string errMsg) nothrow;

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
    final void pollMessages(scope InterceptorT interceptor, bool finishOnError = false)
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
                        n.procId = demarshalNumber!int(msg.data[0..4]);
                        size_t l;
                        n.channel = demarshalProtocolString(msg.data[4..$], l);
                        n.payload = demarshalString(msg.data[4+l..$-1]);
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
    void windupResponseStack()
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

    final void putStartupMessage(in BackendParams params)
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

    final void putTerminateMessage()
    {
        write(cast(ubyte)FrontMessageType.Terminate);
        write(4);
        logTrace("Terminate message buffered");
    }

    final void initialize(in BackendParams params)
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
                    authType = demarshalNumber(msg.data[0..4]);
                    if (authType == 0)  // instantly authorized, so we'll get readyForQuery
                        readyForQueryExpected++;
                    else
                        return true;
                }
                else if (msg.type == BackendMessageType.BackendKeyData)
                {
                    m_processId = demarshalNumber(msg.data[0..4]);
                    m_cancellationKey = demarshalNumber(msg.data[4..8]);
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
                    authRes = demarshalNumber(msg.data[0..4]);
                else if (msg.type == BackendMessageType.BackendKeyData)
                {
                    m_processId = demarshalNumber(msg.data[0..4]);
                    m_cancellationKey = demarshalNumber(msg.data[4..8]);
                }
                return false;
            });
        enforce!PsqlClientException(authRes == 0,
            "Expected AuthenticationOk message was not received");
    }

    final void putPasswordMessage(string pw)
    {
        write(cast(ubyte)FrontMessageType.PasswordMessage);
        auto lenPrefix = reserveLen();
        cwrite(pw);
        lenPrefix.fill();
        readyForQueryExpected++;
        logTrace("Password message buffered");
    }

    final void putMd5PasswordMessage(string pw, string user, ubyte[4] salt)
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
        readyForQueryExpected++;
        logTrace("MD5 Password message buffered");
    }

    final void parseNoticeMessage(ubyte[] data, ref Notice n)
    {
        void copyTillZero(ref string dest)
        {
            size_t length;
            dest = demarshalProtocolString(data, length);
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
                    enforce!PsqlClientException(data.length >= 6,
                        "Expected 5 bytes of SQLSTATE code.");
                    n.code[] = cast(char[]) data[0..5];
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

    /// Read from socket to buffer buf exactly buf.length bytes.
    /// Blocks and throws.
    final void read(ubyte[] buf)
    {
        try
        {
            logTrace("reading %d bytes", buf.length);
            // TODO: make sure this code is generic enough for all sockets
            auto r = m_socket.receive(buf);
            assert(r == buf.length);
            //logTrace("received %d bytes", r);
        }
        catch (PsqlSocketException e)
        {
            open = false;
            throw e;
        }
    }

    /// extends writeBuffer if marshalling functor m is lacking space (returns -2)
    final int wrappedMarsh(MarshT)(scope MarshT m)
    {
        int bcount = m(writeBuffer[bufHead .. $]);
        while (bcount == -2)
        {
            writeBuffer.length = writeBuffer.length + 4 * 4096;
            bcount = m(writeBuffer[bufHead .. $]);
        }
        if (bcount > 0)
            bufHead += bcount;
        return bcount;
    }

    /// write numeric type T to write buffer
    final int write(T)(T val)
        if (isNumeric!T)
    {
        return wrappedMarsh((ubyte[] buf) => marshalFixedField(buf, val));
    }

    /// Reserve space in write buffer for length prefix and return
    /// struct that automatically fills it from current buffer offset position.
    final auto reserveLen(T = int)()
        if (isNumeric!T && !isUnsigned!T)
    {
        static struct Len
        {
            PSQLConnection con;
            int idx;    // offset of length prefix word in writeBuffer

            /// calculate and write length prefix
            void fill(bool includeSelf = true)
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
                auto res = marshalFixedField(con.writeBuffer[idx .. idx+T.sizeof], len);
                assert(res == T.sizeof);
            }

            /// write some specific number
            void write(T v)
            {
                auto res = marshalFixedField(con.writeBuffer[idx .. idx+T.sizeof], v);
                assert(res == T.sizeof);
            }
        }

        Len l = Len(this, bufHead);
        bufHead += T.sizeof;
        return l;
    }

    final int write(string s)
    {
        return wrappedMarsh((ubyte[] buf) => marshalStringField(buf, s));
    }

    final int cwrite(string s)
    {
        return wrappedMarsh((ubyte[] buf) => marshalCstring(buf, s));
    }

    /// read exactly one message from the socket
    final Message readOneMessage()
    {
        Message res;
        ubyte[1] type;
        read(type);
        res.type = cast(BackendMessageType) type[0];
        logTrace("Got message of type %s", res.type.to!string);
        ubyte[4] length_arr;
        read(length_arr);
        int length = demarshalNumber(length_arr) - 4;
        enforce!PsqlClientException(length >= 0, "Negative message length");
        ubyte[] data;
        if (length > 0)
        {
            data = new ubyte[length];
            read(data);
        }
        res.data = data;
        return res;
    }
}
