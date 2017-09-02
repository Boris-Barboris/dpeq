/**
Connection.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.connection;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.range;
import std.socket;

import dpeq.constants;
import dpeq.exceptions;
import dpeq.type;
import dpeq.marshalling;



/// I don't want to care what sockets you use,
/// just make them duck-type and exception-compatible with this one
final class StdSocket
{
    protected Socket m_socket;

    this(string host, ushort port)
    {
        m_socket = new TcpSocket();
        m_socket.connect(new InternetAddress(host, port));
    }

    void close()
    {
        m_socket.shutdown(SocketShutdown.BOTH);
        m_socket.close();
    }

    // Throw PsqlSocketException when something bad happens
    auto send(const(ubyte)[] buf)
    {
        auto r = m_socket.send(buf);
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on send");
        return r;
    }

    // Throw PsqlSocketException when something bad happens
    auto receive(ubyte[] buf)
    {
        auto r = m_socket.receive(buf);
        if (r == 0 && buf.length > 0)
            throw new PsqlSocketException("Connection closed");
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on recieve");
        return r;
    }
}



/// Connection and authorization parameters
struct BackendParams
{
    string host;
    ushort port = cast(ushort)5432;
    string user;
    string password;
    string database;
}


/// Message, recieved from backend
struct Message
{
    BackendMessageType type;

    /// raw network-order (big-endian) byte array, without first 4
    /// bytes wich represent message body length in original protocol
    ubyte[] data;
}

private void nop_logger(T...)(lazy T vals) {}

/// assign some nop function to logDebug when you're fine with results
class PSQLConnection(
    SocketT = StdSocket,
    alias logDebug = nop_logger,
    alias logError = nop_logger)
{
    protected
    {
        SocketT socket;
        ubyte[] writeBuffer;
        int bufHead = 0;
        bool open = false;

        /// number of expected readyForQuery responses
        int readyForQueryExpected = 0;

        /// counters to generate unique prepared statement and portal ids
        int preparedCounter = 0;
        int portalCounter = 0;
    }

    invariant
    {
        assert(readyForQueryExpected >= 0);
        assert(bufHead >= 0);
    }

    /// Connection is open when it is authorized and socket was alive last time
    /// it was checked.
    @property bool isOpen() { return open; }

    string getNewPreparedName()
    {
        return (preparedCounter++).to!string;
    }

    string getNewPortalName()
    {
        return (portalCounter++).to!string;
    }

    /// allocate memory, start TCP connection and authorize
    this(in BackendParams bp)
    {
        writeBuffer.length = 4 * 4096;     // liberal procurement
        try
        {
            logDebug("Trying to open TCP connection to PSQL");
            socket = new SocketT(bp.host, bp.port);
            logDebug("Success");
        }
        catch (Exception e)
        {
            throw new PsqlSocketException(e.msg);
        }
        scope(failure) socket.close();
        initialize(bp);
        open = true;
    }

    /// notify backend and close socket. It may throw when socket is already
    /// closed, so be aware.
    void terminate()
    {
        ensureOpen();
        open = false;
        try
        {
            putTerminateMessage();
            flush();
            socket.close();
        }
        catch (Exception e)
        {
            logError("Exception caught while terminating PSQL connection: ", e.msg);
        }
    }

    /// flush writeBuffer into the socket. This one blocks/yields.
    void flush()
    {
        try
        {
            // does not block if zero length:
            // https://github.com/vibe-d/vibe-core/blob/master/source/vibe/core/net.d#L607
            auto w = socket.send(writeBuffer[0..bufHead]);
            while (bufHead - w > 0)
                w += socket.send(writeBuffer[w..bufHead]);
            logDebug("flushed %d bytes: %s", w, writeBuffer[0..bufHead].to!string);
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
    void discard()
    {
        bufHead = 0;
    }

    /// Save write buffer cursor in order to be able to restore it in case of errors.
    /// Use it to prevent sending junk to backend when something goes wrong during
    /// marshalling or message creation.
    auto saveBuffer()
    {
        struct WriteCursor
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
    void ensureOpen()
    {
        enforce!PsqlConnectionClosedException(open, "Connection is not open");
    }


    ////////////////////////////////////////
    // All sorts of messages
    // https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html
    ///////////////////////////////////////


    /** Put Bind message into write buffer.
    * `parameters` is input range of marshalling delegates.
    */
    void putBindMessage(FR, PR, RR)
        (string portal, string prepared, scope FR formatCodes, scope PR parameters,
        scope RR resultFormatCodes)
    if (isInputRange!FR && is(ElementType!FR == FormatCode) &&
        isInputRange!RR && is(ElementType!RR == FormatCode) &&
        isInputRange!PR && is(ElementType!PR == int delegate(ubyte[])))
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
            wrappedMarsh(param);
            paramPrefix.fill();
            pcount++;
        }
        pcountPrefix.write(pcount);

        // result format codes
        short rcount = 0;
        auto rcolPrefix = reserveLen!short();
        foreach (FormatCode fcode; resultFormatCodes)
        {
            write(cast(short)fcode);
            rcount++;
        }
        fcolPrefix.write(rcount);

        lenTotal.fill();
        logDebug("Bind message sent");
    }

    /// put Close message into write buffer.
    /// `closeWhat` is 'S' for prepared statement and
    /// 'P' for portal.
    void putCloseMessage(char closeWhat, string name)
    {
        ensureOpen();
        assert(closeWhat == 'S' || closeWhat == 'P');
        write(cast(ubyte)FrontMessageType.Close);
        auto lenTotal = reserveLen();
        write(cast(ubyte)closeWhat);
        cwrite(name);
        lenTotal.fill();
        logDebug("Close message sent");
    }

    void putExecuteMessage(string portal = "", int maxRows = 0)
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Execute);
        auto lenTotal = reserveLen();
        cwrite(portal);
        write(maxRows);
        lenTotal.fill();
        readyForQueryExpected++;
        logDebug("Execute message sent");
    }

    void putFlushMessage()
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Flush);
        write(4);
        logDebug("Flush message sent");
    }

    void putParseMessage(PR)(string prepared, string query, scope PR ptypes)
    //  if (isInputRange!PR && is(ElementType!PR == ObjectID) &&
    {
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
        logDebug("Parse message sent");
    }

    void putQueryMessage(string query)
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Query);
        auto lenTotal = reserveLen();
        cwrite(query);
        lenTotal.fill();
        readyForQueryExpected++;
        logDebug("Query message sent");
    }

    alias putSimpleQuery = putQueryMessage;

    /// put Sync message into write buffer
    void putSyncMessage()
    {
        ensureOpen();
        write(cast(ubyte)FrontMessageType.Sync);
        write(4);
        readyForQueryExpected++;
        logDebug("Sync message sent");
    }

    /** this function reads messages from the socket in loop until:
    *     1). if finishOnError is set and ErrorResponse is recieved, function
    *         throws immediately.
    *     2). if ReadyForQuery message is recieved.
    *     3). interceptor delegate provided returned `true`.
    *   Interceptor delegate is used to customize the logic. If the message is
    *   not ReadyForQuery or ErrorResponse, it is passed to interceptor. It may
    *   set bool flag to true and edit string error message, if delayed throw
    *   is required.
    */
    void pollMessages(scope bool delegate(Message, ref bool, ref string) interceptor,
        bool finishOnError = false)
    {
        bool finish = false;
        bool error = false;
        string eMsg;
        bool intError = false;
        string intErrMsg;

        while (!finish)
        {
            Message msg = readOneMessage();

            with (BackendMessageType)
            switch (msg.type)
            {
                case ErrorResponse:
                    error = true;
                    eMsg.reserve(256);
                    handleErrorMessage(msg.data, eMsg);
                    if (finishOnError)
                        finish = true;
                    break;
                case ReadyForQuery:
                    enforce!PsqlClientException(readyForQueryExpected > 0,
                        "Unexpected ReadyForQuery message");
                    readyForQueryExpected--;
                    finish = true;
                    break;
                default:
                    if (interceptor)
                        finish |= interceptor(msg, intError, intErrMsg);
            }
        }

        if (error)
            throw new PsqlErrorResponseException(eMsg);
        if (intError)
            throw new PsqlErrorResponseException(intErrMsg);
    }

    // Protected section for functions that will probably never be used by
    // client code directly. If you need them, inherit them.
protected:

    void putStartupMessage(in BackendParams params)
    {
        auto lenPrefix = reserveLen();
        write(0x0003_0000);  // protocol v3
        cwrite("user");
        cwrite(params.user);
        cwrite("database");
        cwrite(params.database);
        write(cast(ubyte)0);
        lenPrefix.fill();
        logDebug("Startup message sent");
    }

    void putTerminateMessage()
    {
        write(cast(ubyte)FrontMessageType.Terminate);
        write(4);
        logDebug("Terminate message sent");
    }

    void initialize(in BackendParams params)
    {
        putStartupMessage(params);
        flush();

        int authType = -1;

        pollMessages((Message msg, ref bool e, ref string eMsg) {
                if (msg.type == BackendMessageType.Authentication)
                {
                    enforce!PsqlClientException(authType == -1,
                        "Unexpected second Authentication message from backend");
                    authType = demarshalNumber(msg.data[0..4]);
                    if (authType == 0)  // instantly authorized, so we'll get readyForQuery
                        readyForQueryExpected++;
                    else
                        return true;
                }
                return false;
            }, true);

        enforce!PsqlClientException(authType != -1,
            "No Authentication message recieved");
        switch (authType)
        {
            case 0:
                // AuthenticationOk, lul
                logDebug("Succesfully authorized");
                return;
            case 3:
                // cleartext password
                putPasswordMessage(params.password);
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
                return false;
            });
        enforce!PsqlClientException(authRes == 0, "No AuthenticationOk message");
    }

    void putPasswordMessage(string pw)
    {
        write(cast(ubyte)FrontMessageType.PasswordMessage);
        auto lenPrefix = reserveLen();
        cwrite(pw);
        lenPrefix.fill();
        readyForQueryExpected++;
        logDebug("Password message sent");
    }

    void handleErrorMessage(ubyte[] data, ref string msg)
    {
        void copyTillZero()
        {
            //logDebug("copyTillZero " ~ data.to!string);
            int idx = 0;
            while (data[idx])
            {
                msg ~= cast(char)data[idx];
                idx++;
            }
            //logDebug("idx %d", idx);
            data = data[idx+1..$];
        }

        void discardTillZero()
        {
            //logDebug("discardTillZero " ~ data.to!string);
            if (!data.length)
                return;
            int idx = 0;
            while (idx < data.length)
                if (data[idx])
                    idx++;
                else
                    break;
            data = data[idx+1..$];
            //logDebug("discardTillZero2 " ~ data.to!string);
        }

        // would be nice to handle:
        // https://www.postgresql.org/docs/9.5/static/errcodes-appendix.html
        while (data.length > 0)
        {
            //logDebug("data = " ~ (cast(string)data).to!string);
            char fieldType = cast(char) data[0];
            data = data[1..$];
            switch (fieldType)
            {
                case 'S':
                    copyTillZero();
                    msg ~= " ";
                    break;
                case 'M':
                    copyTillZero();
                    msg ~= " ";
                    break;
                default:
                    discardTillZero();
            }
        }
    }

    /// Read from socket to buffer buf exactly buf.length bytes.
    /// Blocks and throws.
    void read(ubyte[] buf)
    {
        try
        {
            logDebug("reading %d bytes", buf.length);
            // TODO: make sure this code is generic enough for all sockets
            auto r = socket.receive(buf);
            if (r == Socket.ERROR)
                throw new PsqlSocketException("socket.recieve returned error");
            if (r == 0)
            {
                open = false;
                throw new PsqlSocketException("connection closed");
            }
            assert(r == buf.length);
            logDebug("read %d bytes", r);
        }
        catch (Exception e)
        {
            throw new PsqlSocketException(e.msg);
        }
    }

    /// extends writeBuffer if marshalling delegate m is lacking space (returns -2)
    int wrappedMarsh(scope int delegate() m)
    {
        int bcount = m();
        while (bcount == -2)
        {
            writeBuffer.length = writeBuffer.length + 4 * 4096;
            bcount = m();
        }
        return bcount;
    }

    int wrappedMarsh(scope int delegate(ubyte[]) m)
    {
        int bcount = m(writeBuffer[bufHead .. $]);
        while (bcount == -2)
        {
            writeBuffer.length = writeBuffer.length + 4 * 4096;
            bcount = m(writeBuffer[bufHead .. $]);
        }
        bufHead += bcount;
        return bcount;
    }

    /// write numeric type T to write buffer
    int write(T)(T val)
        if (isNumeric!T)
    {
        int w = wrappedMarsh(() => marshalFixed(writeBuffer[bufHead .. $], val));
        if (w > 0)
            bufHead += w;
        return w;
    }

    /// Reserve space in write buffer for length prefix and return
    /// struct that automatically fills it from current buffer offset position.
    auto reserveLen(T = int)()
        if (isNumeric!T && !isUnsigned!T)
    {
        struct Len
        {
            PSQLConnection con;
            int idx;    // offset of length prefix word in writeBuffer
            debug bool used = false;

            /// calculate and write length prefix
            void fill(bool includeSelf = true)
            {
                assert(!used, "Already filled this prefix");
                used = true;
                T len = (con.bufHead - idx).to!T;
                if (!includeSelf)
                {
                    len -= T.sizeof.to!T;
                    assert(len >= 0);
                }
                else
                    assert(len >= T.sizeof);
                logDebug("writing length of %d bytes to index %d", len, idx);
                auto res = marshalFixed(con.writeBuffer[idx .. idx+T.sizeof], len);
                assert(res == T.sizeof);
            }

            /// write some specific number
            void write(T v)
            {
                assert(!used, "Already filled this prefix");
                used = true;
                auto res = marshalFixed(con.writeBuffer[idx .. idx+T.sizeof], v);
                assert(res == T.sizeof);
            }
        }

        Len l = Len(this, bufHead);
        bufHead += T.sizeof;
        return l;
    }

    int write(string s)
    {
        int w = wrappedMarsh(() => marshalString(writeBuffer[bufHead .. $], s));
        if (w > 0)
            bufHead += w;
        return w;
    }

    int cwrite(string s)
    {
        int w = wrappedMarsh(() => marshalCstring(writeBuffer[bufHead .. $], s));
        if (w > 0)
            bufHead += w;
        return w;
    }

    /// read exactly one message from the socket
    Message readOneMessage()
    {
        Message res;
        ubyte[1] type;
        read(type);
        res.type = cast(BackendMessageType) type[0];
        logDebug("Got message of type " ~ res.type.to!string);
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
