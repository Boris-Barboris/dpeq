/**
Main PSQLConnection class.

Copyright: Boris-Barboris 2017-2020.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.connection;

import std.exception: enforce;
import std.conv: to;

import dpeq.authentication;
import dpeq.constants;
import dpeq.exceptions;
import dpeq.messages;
import dpeq.serialization;
import dpeq.transport;



enum SSLPolicy
{
    NEVER,      /// never use SSL.
    PREFER,     /// use SSL ONLY if both the transport and backend support it.
    REQUIRED    /// always use SSL, abort if transport or backend does not support it.
}

enum PollAction
{
    CONTINUE,
    BREAK       /// message polling loop will exit.
}

/// Callback of this signature is invoked when a NotificationResponse
/// message is received inside pollMessages call. Any Throwable thrown
/// will close the connection and will be rethrown from pollMessages.
alias NotificationCallback = PollAction delegate(
    PSQLConnection receiver, NotificationResponse message);

/// Callback of this signature is invoked when a NoticeResponse
/// message is received inside pollMessages call. Any Throwable thrown
/// will close the connection and will be rethrown from pollMessages.
alias NoticeCallback = PollAction delegate(
    PSQLConnection receiver, NoticeOrError message);

/// Callback of this signature is invoked when a ParameterStatus
/// message is received inside pollMessages call, right after updating
/// connection's parameterStatuses map. Any Throwable thrown
/// will close the connection and will be rethrown from pollMessages.
/// This callback cannot stop the poll loop.
alias ParameterStatusCallback = void delegate(
    PSQLConnection receiver, ParameterStatus message);

/// Callback of this signature is invoked when any known
/// message besides NotificationResponse, NoticeResponse, ParameterStatus or
/// ReadyForQuery is received inside pollMessages call. Any Throwable thrown
/// will close the connection and will be rethrown from pollMessages.
alias PollCallback = PollAction delegate(
    PSQLConnection receiver, RawBackendMessage message);

/// Reason the 'PSQLConnection.pollMessages' method has returned.
enum PollResult
{
    RFQ_RECEIVED,                   /// ReadyForQuery message was received.
    POLL_CALLBACK_BREAK,            /// pollCallback returned BREAK.
    NOTIFICATION_CALLBACK_BREAK,    /// notificationCallback returned BREAK.
    NOTICE_CALLBACK_BREAK           /// noticeCallback returned BREAK.
}


/**
PSQLConnection object sits on top of a ITransport and is responsible for
protocol flow: startup, SSL negotiation, authentication start, message loop,
notification dispatch.
*/
class PSQLConnection
{
    private
    {
        /// false unless m_transport.close() was called.
        bool m_closed = false;
        /// false until 'handshakeAndAuthenticate' has succeeded.
        bool m_authenticated = false;

        ITransport m_transport;
        SSLPolicy m_sslPolicy;
        TransactionStatus m_lastTransactionStatus;

        // cancellation-related fields
        BackendKeyData m_backendKeyData;

        string[string] m_parameterStatuses;
    }

    /// Initialize connection object with transport and ssl policy.
    /// Transport may or may not be already connected to backend.
    this(ITransport transport, SSLPolicy sslPolicy)
    {
        assert(transport);
        m_transport = transport;
        m_sslPolicy = sslPolicy;
        enforce!PSQLClientException(
            !(m_sslPolicy == SSLPolicy.REQUIRED && !m_transport.supportsSSL),
            "Transport does not support SSL when SSL policy is set to REQUIRED");
    }

    final @property @safe nothrow
    {
        /// Backend process ID. Required for CancelRequest message.
        /// Returns -1 for unauthenticated connection.
        BackendKeyData backendKeyData() const { return m_backendKeyData; }

        /// ITransport instance this connection was constructed with.
        ITransport transport() { return m_transport; }

        /// Transaction status, reported by the last received ReadyForQuery message.
        /// For a brand new connection TransactionStatus.IDLE is returned.
        TransactionStatus lastTransactionStatus() const { return m_lastTransactionStatus; }

        /// Connection is open after successfull 'handshakeAndAuthenticate' call and
        /// before 'close()' call.
        bool isOpen() const { return !m_closed && m_authenticated; }

        bool closed() const { return m_closed; }

        bool authenticated() const { return m_authenticated; }

        /// Last known state of parameter statuses.
        /// Dict is updated from ParameterStatus messages during pollMessages.
        const(string[string]) parameterStatuses() const { return m_parameterStatuses; }
    }

    /// Given 'startupParams' dict that contains at least 'user' key and
    /// an authenticator, perform protocol startup, SSL negotiation and
    /// authentication. Function returns when the first ReadyForQuery is received
    /// or closes the transport and throws on error.
    void handshakeAndAuthenticate(
        string[string] startupParams, IPSQLAuthenticator auth)
    {
        assert(auth);
        enforce!PSQLClientException(!m_closed, "connection already closed");
        enforce!PSQLClientException(!m_authenticated, "connection already authenticated");
        scope(failure) close(false);
        enforceSSLPolicy(m_transport);
        m_transport.send(buildStartupMessage(startupParams));
        RawBackendMessage firstResponse = receiveBackendMessage(m_transport);
        if (firstResponse.type == BackendMessageType.ErrorResponse)
            throw new PSQLErrorResponseException(NoticeOrError.parse(firstResponse.data));
        enforce!PSQLProtocolException(firstResponse.type == BackendMessageType.Authentication);
        AuthenticationMessage authResponse =
            AuthenticationMessage.parse(firstResponse.data);
        if (authResponse.protocol != AUTHENTICATION_SUCCESS)
            auth.authenticate(m_transport, authResponse, startupParams);

        PollAction pollStarupMessages(PSQLConnection that, RawBackendMessage msg)
        {
            if (msg.type == BackendMessageType.BackendKeyData)
                m_backendKeyData = BackendKeyData.parse(msg.data);
            if (msg.type == BackendMessageType.ParameterStatus)
            {
                ParameterStatus status = ParameterStatus.parse(msg.data);
                m_parameterStatuses[status.name] = status.value;
            }
            return PollAction.CONTINUE;
        }

        while (pollMessages(&pollStarupMessages) != PollResult.RFQ_RECEIVED) {}
        m_authenticated = true;
    }

    /// Unconditionally close the underlying transport.
    /// If 'sendTerminate' is true, sends Terminate message to bakend right
    /// berore closing the transport. Repeated calls do nothing.
    final void close(bool sendTerminate = true) nothrow
    {
        if (m_closed)
            return;
        m_closed = true;
        scope(exit) m_transport.close();
        if (sendTerminate)
        {
            try
            {
                m_transport.send(buildTerminateMessage().data);
            }
            catch (Exception e) {}
        }
    }

    /// Open new transport and connect it to the same backend as this connection is
    /// bound to, send CancelRequest and close the cancellation transport socket.
    /// Throws if the connection was never authenticated.
    /// Thread-safe.
    final void cancelRequest()
    {
        enforce!PSQLClientException(m_authenticated,
            "connection is not authenticated");
        ITransport clone = m_transport.duplicate();
        scope(exit) clone.close();
        enforceSSLPolicy(clone);
        clone.send(buildCancelRequestMessage(m_backendKeyData));
    }

    protected void enforceSSLPolicy(ITransport t)
    {
        if (!t.supportsSSL && m_sslPolicy == SSLPolicy.REQUIRED)
            throw new PSQLClientException("transport does not support SSL");
        if (t.supportsSSL && m_sslPolicy >= SSLPolicy.PREFER)
        {
            t.send(buildSSLRequestMessage());
            ubyte[1] response;
            t.receive(response[]);
            switch (response[0])
            {
                case 'S':
                    t.performSSLHandshake();
                    break;
                case 'N':
                    enforce!PSQLClientException(m_sslPolicy != SSLPolicy.REQUIRED,
                        "Required SSL encryption is not offered by backend");
                    break;
                default:
                    throw new PSQLProtocolException(
                        "Unexpected response to SSLRequest message");
            }
        }
    }

    /// NotificationResponse messages will be passed to this
    /// callback during 'pollMessages' call.
    /// https://www.postgresql.org/docs/9.5/static/sql-notify.html
    NotificationCallback notificationCallback;

    /// NoticeResponse messages will be passed
    /// to this callback during 'pollMessages' call.
    NoticeCallback noticeCallback;

    /// ParameterStatus messages will be passed to this callback during 'pollMessages' call.
    /// For example, 'server_encoding', 'server_version' and 'TimeZone' parameter values are pushed
    /// to client after successfull authentication.
    /// This callback cannot interrupt 'pollMessages' loop.
    ParameterStatusCallback parameterStatusCallback;

    /** Repeatedly read messages from the transport until:
      1). ReadyForQuery message is received.
      2). pollCallback returns BREAK.
      3). notificationCallback returns BREAK.
      4). noticeCallback returns BREAK.
      5). some callback throws. In this case connection will be closed.
    */
    final PollResult pollMessages(scope PollCallback pollCallback)
    {
        enforce!PSQLClientException(!m_closed, "connection is closed");
        scope(failure) close();
        while(true)
        {
            // This materialized whole message in memory. Not suitable for
            // large objects, but that is just and unsupported edge case.
            RawBackendMessage msg = receiveMessage();
            switch (msg.type)
            {
                case BackendMessageType.ReadyForQuery:
                {
                    m_lastTransactionStatus =
                        ReadyForQuery.parse(msg.data).transactionStatus;
                    return PollResult.RFQ_RECEIVED;
                }
                case BackendMessageType.NotificationResponse:
                {
                    if (notificationCallback)
                    {
                        PollAction action = notificationCallback(
                            this, NotificationResponse.parse(msg.data));
                        if (action == PollAction.BREAK)
                            return PollResult.NOTIFICATION_CALLBACK_BREAK;
                    }
                    break;
                }
                case BackendMessageType.NoticeResponse:
                {
                    if (noticeCallback)
                    {
                        PollAction action = noticeCallback(
                            this, NoticeOrError.parse(msg.data));
                        if (action == PollAction.BREAK)
                            return PollResult.NOTICE_CALLBACK_BREAK;
                    }
                    break;
                }
                case BackendMessageType.ParameterStatus:
                {
                    ParameterStatus psMsg = ParameterStatus.parse(msg.data);
                    m_parameterStatuses[psMsg.name] = psMsg.value;
                    if (parameterStatusCallback)
                        parameterStatusCallback(this, psMsg);
                    break;
                }
                default:
                {
                    if (pollCallback)
                    {
                        PollAction action = pollCallback(this, msg);
                        if (action == PollAction.BREAK)
                            return PollResult.POLL_CALLBACK_BREAK;
                    }
                }
            }
        }
    }

    /// Receive one message from backend without passing it to any special callbacks.
    /// Caller is responsible for careful state management. If possible, use 'pollMessages'.
    /// Any internal exception will result in close() being called.
    RawBackendMessage receiveMessage()
    {
        enforce!PSQLClientException(!m_closed, "connection is closed");
        scope(failure) close();
        return receiveBackendMessage(m_transport);
    }

    /// Send one message to underlying transport.
    /// Any internal exception will result in close() being called.
    void sendMessage(const RawFrontendMessage message)
    {
        enforce!PSQLClientException(!m_closed, "connection is closed");
        scope(failure) close();
        m_transport.send(message.data);
    }
}
