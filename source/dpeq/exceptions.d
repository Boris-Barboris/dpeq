/**
Exceptions, thrown by dpeq code.

Copyright: Boris-Barboris 2017-2019.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.exceptions;

import std.exception: basicExceptionCtors;

public import dpeq.messages: NoticeOrError;


class PSQLClientException: Exception
{
    mixin basicExceptionCtors;
}

class PSQLAuthenticationException: PSQLClientException
{
    mixin basicExceptionCtors;
}

class PSQLSerializationException: PSQLClientException
{
    mixin basicExceptionCtors;
}

/// Exception mostly implies violated assumptions about the form of the data
/// being deserialized. Both frontend and backend can be guilty.
class PSQLDeserializationException: PSQLClientException
{
    mixin basicExceptionCtors;
}

/// Message flow or structure violation, backend is probably to blame.
class PSQLProtocolException: PSQLClientException
{
    mixin basicExceptionCtors;
}

class PSQLSocketException: Exception
{
    mixin basicExceptionCtors;
}

class PSQLConnectionClosedException: PSQLSocketException
{
    mixin basicExceptionCtors;
}

/// Thrown by $(D dpeq.PSQLConnection) when ErrorResponse message is received.
class PSQLErrorResponseException: Exception
{
    /// Contents of ErrorResponse message that caused this exception.
    NoticeOrError error;

    @safe pure nothrow this(NoticeOrError e,
                            Throwable next,
                            string file =__FILE__,
                            size_t line = __LINE__)
    {
        super(e.message, next, file, line);
        error = e;
    }

    @safe pure nothrow this(NoticeOrError e,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(e.message, file, line, next);
        error = e;
    }
}
