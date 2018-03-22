/**
Exceptions, thrown by dpeq code.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.exceptions;

import dpeq.result: Notice;


/// mixes in standard exception constructors that call super correctly
mixin template ExceptionConstructors()
{
    @safe pure nothrow this(string message,
                            Throwable next,
                            string file =__FILE__,
                            size_t line = __LINE__)
    {
        super(message, next, file, line);
    }

    @safe pure nothrow this(string message,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(message, file, line, next);
    }
}

class PsqlClientException: Exception
{
    mixin ExceptionConstructors;
}

class PsqlMarshallingException: PsqlClientException
{
    mixin ExceptionConstructors;
}

class PsqlConnectionClosedException: PsqlSocketException
{
    mixin ExceptionConstructors;
}

class PsqlSocketException: Exception
{
    mixin ExceptionConstructors;
}

/// Thrown by $(D dpeq.PSQLConnection.pollMessages) when ErrorResponse message
/// is received (immediately after parse or delayed until ReadyForQuery message).
class PsqlErrorResponseException: Exception
{
    /// Contents of ErrorResponse message that caused this exception.
    Notice notice;

    @safe pure nothrow this(Notice n,
                            Throwable next,
                            string file =__FILE__,
                            size_t line = __LINE__)
    {
        super(n.message, next, file, line);
        notice = n;
    }

    @safe pure nothrow this(Notice n,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(n.message, file, line, next);
        notice = n;
    }
}
