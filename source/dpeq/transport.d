/**
Default socket type for Connection.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.transport;

import core.time: Duration;
import core.stdc.errno: errno, EINTR, EWOULDBLOCK, EAGAIN;

import std.conv: to;
import std.socket;
import std.exception: enforce;

import dpeq.exceptions;


/// Bi-directional transport with no control operations.
interface IOpenTransport
{
    /// Send 'data' buffer as a whole to the socket/operating system or throw.
    /// Implementation MUST handle all looping and signal/interrupt handling logic.
    /// Any exception thrown from 'send' will cause 'close' call from PSQLConnection.
    void send(const(ubyte)[] data);

    /// Receive 'dest.length' bytes from the underlying connection or throw.
    /// Any exception thrown from 'receive' will cause 'close' call from PSQLConnection.
    void receive(ubyte[] dest);
}

/// Generic transport interface, used by PSQLConnection object.
/// Implementations may choose to support asynchronous IO (vibe-d).
interface ITransport: IOpenTransport
{
    /// Construct and open connection to the same endpoint. This call
    /// is used to open new connection and issue cancellation requests.
    /// Transport MUST NOT perform SSL handshake on the new instance,
    /// a duplicate is expected to be a just established tcp/unix socket
    /// connection.
    /// MUST be thread-safe.
    ITransport duplicate();

    /// MUST free all underlying resources. Will be called at most once by
    /// PSQLConnection. No other calls will be issued by connection after close().
    void close() nothrow;

    /// MUST return true only if 'performSSLHandshake' is implemented.
    @property bool supportsSSL() nothrow;

    /// Perform standard SSL handshake with the backend, throw on error.
    /// Any exception thrown from 'performSSLHandshake' will cause 'close'
    /// call from PSQLConnection.
    void performSSLHandshake();
}


struct ConnectParameters
{
    /// destination hostname, IPv4 address or unix socket path. Example:
    /// /var/run/postgresql/.s.PGSQL.5432
    string host;
    ushort port = 5432;
}

/// Example $(D std.socket.Socket) wrapper, compatible with PSQLConnection.
/// Does NOT implement SSL.
class StdSocketTransport: ITransport
{
    private
    {
        const ConnectParameters m_connectParams;
        Socket m_socket;
        Address m_address;
    }

    /// Underlying socket instance.
    @property Socket socket() { return m_socket; }

    this(ConnectParameters params)
    {
        m_connectParams = params;
        enforce(params.host.length > 0, "empty host address string");
        if (params.host[0] == '/')
        {
            version(Posix)
            {
                m_address = new UnixAddress(params.host);
                m_socket = new Socket(AddressFamily.UNIX, SocketType.STREAM);
            }
            else
                throw new Exception("UNIX sockets unavailable on non-Posix OS");
        }
        else
        {
            m_address = new InternetAddress(params.host, params.port);
            m_socket = new TcpSocket();
            m_socket.setKeepAlive(1200, 1200);
        }
    }

    void connect()
    {
        m_socket.connect(m_address);
    }

    StdSocketTransport duplicate()
    {
        StdSocketTransport clone = new StdSocketTransport(m_connectParams);
        clone.connect();
        return clone;
    }

    void close() nothrow @nogc
    {
        m_socket.shutdown(SocketShutdown.BOTH);
        m_socket.close();
    }

    void send(const(ubyte)[] buf)
    {
        while (buf.length > 0)
        {
            auto sent = m_socket.send(buf);
            assert(sent <= buf.length);
            if (sent <= Socket.ERROR)
            {
                version(Posix)
                {
                    if (errno == EINTR)
                        continue;
                }
                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    throw new PSQLSocketException("send timeout");
                throw new PSQLSocketException(lastSocketError());
            }
            else
                buf = buf[sent .. $];
        }
    }

    @property void receiveTimeout(Duration rhs)
    {
        m_socket.setOption(SocketOptionLevel.SOCKET,
            SocketOption.RCVTIMEO, rhs);
    }

    @property Duration receiveTimeout()
    {
        Duration res;
        m_socket.getOption(SocketOptionLevel.SOCKET,
            SocketOption.RCVTIMEO, res);
        return res;
    }

    @property void sendTimeout(Duration rhs)
    {
        m_socket.setOption(SocketOptionLevel.SOCKET,
            SocketOption.SNDTIMEO, rhs);
    }

    @property Duration sendTimeout()
    {
        Duration res;
        m_socket.getOption(SocketOptionLevel.SOCKET,
            SocketOption.SNDTIMEO, res);
        return res;
    }

    void receive(ubyte[] dest)
    {
        while (dest.length > 0)
        {
            auto received = m_socket.receive(dest);
            static assert(Socket.ERROR < 0);
            if (received <= Socket.ERROR)
            {
                version(Posix)
                {
                    if (errno == EINTR)
                        continue;
                }
                if (errno == EWOULDBLOCK || errno == EAGAIN)
                    throw new PSQLSocketException("receive timeout");
                throw new PSQLSocketException(lastSocketError());
            }
            else if (received == 0)
                throw new PSQLConnectionClosedException("remote peer closed connection");
            else
                dest = dest[received .. $];
        }
    }

    @property bool supportsSSL() nothrow { return false; }

    void performSSLHandshake()
    {
        assert(0, "not implemented and not supposed to be called");
    }
}