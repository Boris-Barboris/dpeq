/**
Default socket type for Connection.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.socket;

import std.socket;

import dpeq.exceptions;



/**
$(D std.socket.Socket) wrapper wich is compatible with PSQLConnection.
If you want to use custom socket type (vibe-d or any other), make it
duck-type and exception-compatible with this one.
*/
final class StdSocket
{
    private Socket m_socket;

    /// Underlying socket instance.
    @property Socket socket() @safe pure { return m_socket; }

    /// Establish connection to backend. Constructor is expected to throw Exception
    /// if anything goes wrong.
    this(string host, ushort port) @trusted
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

    void close() nothrow @nogc @trusted
    {
        m_socket.shutdown(SocketShutdown.BOTH);
        m_socket.close();
    }

    /// Send whole byte buffer or throw. Throws: $(D PsqlSocketException).
    auto send(const(ubyte)[] buf) @trusted
    {
        auto r = m_socket.send(buf);
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on send");
        return r;
    }

    /// Fill byte buffer from the socket completely or throw.
    /// Throws: $(D PsqlSocketException).
    auto receive(ubyte[] buf) @trusted
    {
        auto r = m_socket.receive(buf);
        if (r == 0 && buf.length > 0)
            throw new PsqlSocketException("Connection closed");
        if (r == Socket.ERROR)
            throw new PsqlSocketException("Socket.ERROR on receive: " ~ m_socket.getErrorText());
        return r;
    }
}