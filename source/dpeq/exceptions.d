/**
Exceptions.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.exceptions;


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

class PsqlConnectionClosedException: PsqlSocketException
{
    mixin ExceptionConstructors;
}

class PsqlSocketException: Exception
{
    mixin ExceptionConstructors;
}

class PsqlErrorResponseException: Exception
{
    mixin ExceptionConstructors;
}
