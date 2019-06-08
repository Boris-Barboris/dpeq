/**
Postgres authentication interface and basic implementations.

Copyright: Boris-Barboris 2017-2019.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.authentication;

import std.conv: to;
import std.exception: enforce;

import dpeq.constants;
import dpeq.exceptions;
import dpeq.messages;
import dpeq.serialization;
import dpeq.transport;


/// Generic authenticator interface.
interface IPSQLAuthenticator
{
    /// Exchange messages with backend until AuthenticationOk message is received,
    /// at wich point the method MUST return. Upon encountering ErrorResponse
    /// message this method MUST immediately throw PSQLErrorResponseException.
    /// MUST throw PSQLAuthenticationException immediately if protocol, specified
    /// by backend in 'firstAuthResponse' is not supported.
    void authenticate(
        IOpenTransport transport, AuthenticationMessage firstAuthResponse,
        string[string] startupParameters);
}


/// Cleartext or MD5 password authentication.
final class PasswordAuthenticator: IPSQLAuthenticator
{
    private
    {
        string m_password;
    }

    this(string password)
    {
        m_password = password;
    }

    void authenticate(
        IOpenTransport transport, AuthenticationMessage firstAuthResponse,
        string[string] startupParameters)
    {
        // identify the particular flavor of password authentication.
        switch (firstAuthResponse.protocol)
        {
            case AuthenticationProtocol.CLEARTEXT_PASSWORD:
                transport.send(buildPasswordMessage(m_password).data);
                break;
            case AuthenticationProtocol.MD5_PASSWORD:
                assert("user" in startupParameters, "No 'user' in startupParameters");
                enforce!PSQLProtocolException(firstAuthResponse.data.length == 4);
                ubyte[4] salt = firstAuthResponse.data[];
                transport.send(
                    buildMD5PasswordMessage(
                        startupParameters["user"], m_password, salt).data);
                break;
            default:
                throw new PSQLAuthenticationException(
                    "Expected password authentication type, got " ~
                    firstAuthResponse.protocol.to!string);
        }
        // first message we get back must be ErrorResponse or AuthenticationSuccess
        RawBackendMessage response = receiveBackendMessage(transport);
        switch (response.type)
        {
            case BackendMessageType.Authentication:
                enforce!PSQLProtocolException(response.data.length > 0);
                enforce!PSQLProtocolException(response.data[0] == AUTHENTICATION_SUCCESS);
                return;
            case BackendMessageType.ErrorResponse:
                NoticeOrError error = NoticeOrError.parse(response.data);
                throw new PSQLErrorResponseException(error);
            default:
                throw new PSQLProtocolException(
                    "unexpected message of type " ~ response.type.to!string);
        }
    }
}