/**
Exceptions.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.exceptions;


class PsqlClientException: Exception
{
    @safe pure nothrow this(string message,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(message, file, line, next);
    }
}

class PsqlConnectionClosedException: PsqlClientException
{
    @safe pure nothrow this(string message,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(message, file, line, next);
    }
}

class PsqlSocketException: Exception
{
    @safe pure nothrow this(string message,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(message, file, line, next);
    }
}

class PsqlErrorResponseException: Exception
{
    @safe pure nothrow this(string message,
                            string file =__FILE__,
                            size_t line = __LINE__,
                            Throwable next = null)
    {
        super(message, file, line, next);
    }
}
