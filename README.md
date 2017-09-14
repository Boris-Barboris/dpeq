# DPEQ - native PSQL extended query protocol client for D programming language

DPEQ is a source library aimed at providing low-level communication interface,
with minimal abstraction and encapsulation.
PSQL's native interface is very performant, and it's a shame there is currently
no native solution for D.

You should probably get yourself familiar with it, if you want to use it.   
https://www.postgresql.org/docs/9.5/static/protocol.html   
https://www.postgresql.org/docs/9.5/static/protocol-flow.html   
https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html   

Many thanks to authors of https://github.com/teamhackback/hb-ddb and
https://github.com/pszturmaj/ddb, wich gave this library some inspiration.

## Source structure
* source/dpeq/command.d - medium-level interfaces to connection. You can find example
  *PreparedStatement* and *Portal* class implementations there. *getQueryResults* function
  from this module is bread and butter for response demarshalling. *blockToVariants*
  and *blockToTuples* are example demarshalling implementations that allow you to
  lazily work with QueryResult's.
* source/dpeq/connection.d - buffered message streamer *PSQLConnection* is used
  to send and recieve messages. It's a class template with customazable socket
  type (so you can easily use it with vibe-d) and logging functions.
* source/dpeq/constants.d - sorry ass header file. It's limited content is caused
  by little time put into it. I didn't need any types besides integers, bool and
  string. Pull requests regarding type support are more than welcome.
* source/dpeq/exceptions.d - pretty straightforward.
* source/dpeq/marshalling.d - templated (de)marshalling code, used throughout
  dpeq. This is the place to research for ways to inject custom type behaviour.
* source/dpeq/schema.d - structures that describe query results are put here.

## How to use vibe-d sockets
Wrap them into class and pass it as a template parameter to PSQLConnection.
```
private final class SocketT
{
    TCPConnection m_con;

    this(string host, ushort port)
    {
        m_con = connectTCP(host, port);
    }

    void close()
    {
        m_con.close();
    }

    auto send(const(ubyte)[] buf)
    {
        try
        {
            return m_con.write(buf, IOMode.all);
        }
        catch (Exception e)
        {
            throw new PsqlSocketException(e.msg);
        }
    }

    auto receive(ubyte[] buf)
    {
        try
        {
            return m_con.read(buf, IOMode.all);
        }
        catch (Exception e)
        {
            throw new PsqlSocketException(e.msg);
        }
    }
}
```

## What types are currently handled by default marshaller template?
SMALLINT, INT, BIGINT, BOOLEAN, VARCHAR/TEXT/CHARACTER, and their Nullable
counterparts. **Please send pull requests for new types!**
To quickly hack them in and test without modifying dpeq, *DefaultFieldMarshaller*,
*VariantConverter* and most marshalling-related templates are extensible, so
you can modify behaviour purely from client code.

## Authrization mechanisms
Trusted (trivial), password and md5 supported currently.

## Examples

Simple examples can be found in tests folder. Otherwise, I recommend investing time into
learning underlying protocol and looking at the source code.

### Login
```
auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
    BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "dbname"));
```
### Transactions
```
void transaction_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "dbname"));
    con.postSimpleQuery("BEGIN;");
    con.flush();
    auto res = con.getQueryResults();
    writeln("Got result ", res);
    assert(res.commandsComplete == 1);
    con.postSimpleQuery("COMMIT;");
    con.flush();
    auto res2 = con.getQueryResults();
    writeln("Got result ", res2);
    assert(res2.commandsComplete == 1);
    con.terminate();
}
```
### Simple query select
```
void select_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    con.postSimpleQuery("SELECT * FROM sessions LIMIT 2;");
    con.flush();
    auto res = con.getQueryResults();
    writeln("Got result ", res);

    // rows is array of imput ranges of lazily-demarshalled variants
    auto rows = blockToVariants(res.blocks[0]);
    foreach (row; rows)
    {
        writeln("iterating over data row");
        foreach (field; row)
        {
            writeln("iterating over data field ", field);
        }
    }
    con.terminate();
}
```
### Extended query select
```

import std.meta;
import std.stdio;

template FormatCodeFromFieldSpec(FieldSpec spec)
{
    enum FormatCodeFromFieldSpec =
        DefaultFieldMarshaller!(spec).formatCode;
}

enum FieldSpec[] SessionSpec = [
    FieldSpec(StaticPgTypes.BIGINT, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.SMALLINT, false),
    FieldSpec(StaticPgTypes.BIGINT, false),
    FieldSpec(StaticPgTypes.BIGINT, false),
    FieldSpec(StaticPgTypes.BIGINT, false),
    FieldSpec(StaticPgTypes.BOOLEAN, false),
    FieldSpec(StaticPgTypes.BOOLEAN, false),
    FieldSpec(StaticPgTypes.VARCHAR, false),
    FieldSpec(StaticPgTypes.INT, false),
];

enum FormatCode[] fullRowFormats =
    [staticMap!(FormatCodeFromFieldSpec, aliasSeqOf!SessionSpec)];

void bind_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    auto ps = new PreparedStatement!(typeof(con))
        (con, "SELECT * FROM sessions LIMIT $1;", null, true);
    auto portal = new Portal!(typeof(con))(ps, 1, true);
    ps.postParseMessage();
    portal.bind!([FieldSpec(StaticPgTypes.BIGINT, false)], fullRowFormats)(3);
    portal.execute(false);
    con.sync();
    con.flush();
    ps.ensureParseComplete();   // not mandatory
    writefln("Parse complete");
    portal.ensureBindComplete(); // you can skip this too
    writefln("Bind complete");

    // this is the call that matters and should be dealt with care
    auto res = con.getQueryResults();
    auto rows = blockToTuples!SessionSpec(res.blocks[0].dataRows);

    // lazy demarshalling of tuples from byte buffers allocated for messages
    foreach (row; rows)
    {
        writeln("iterating over data row");
        writeln("row = ", row);  // prints Tuple!(long, string... value
    }

    portal.postCloseMessage();
    ps.postCloseMessage();
    con.sync();
    con.terminate();
}
```

## Perfomrance

Simple tests on localhost PSQL in docker. Vibe-d http server, 4 threads,
slightly hacked eventcore and vibe-core. 64 concurrent *ab* workers.
All I want to say, if you're careful enough, you should be able to pass some
good data streams. But it remains to be broken.

Dynamically parsed and bound select of all rows (70 items):
```
Concurrency Level:      64
Time taken for tests:   4.497 seconds
Complete requests:      10000
Failed requests:        0
Total transferred:      422310000 bytes
HTML transferred:       420920000 bytes
Requests per second:    2223.53 [#/sec] (mean)
Time per request:       28.783 [ms] (mean)
Time per request:       0.450 [ms] (mean, across all concurrent requests)
Transfer rate:          91701.01 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    0   0.2      0       4
Processing:     1   29  28.5     24     444
Waiting:        1   28  28.5     24     443
Total:          1   29  28.5     24     444
```

Select one row by id, keepalive (note vibe-d related failures):
```
Concurrency Level:      64
Time taken for tests:   4.728 seconds
Complete requests:      100000
Failed requests:        69329
   (Connect: 0, Receive: 0, Length: 69329, Exceptions: 0)
Keep-Alive requests:    100000
Total transferred:      62699536 bytes
HTML transferred:       14231344 bytes
Requests per second:    21150.14 [#/sec] (mean)
Time per request:       3.026 [ms] (mean)
Time per request:       0.047 [ms] (mean, across all concurrent requests)
Transfer rate:          12950.24 [Kbytes/sec] received
```

Not-keepalive:
```
Server Software:        drova_session_manager
Server Hostname:        localhost
Server Port:            9512

Document Path:          /sessions/0a0aad17-abb4-491f-9762-a0509cad465a
Document Length:        464 bytes

Concurrency Level:      64
Time taken for tests:   8.306 seconds
Complete requests:      100000
Failed requests:        0
Total transferred:      60300000 bytes
HTML transferred:       46400000 bytes
Requests per second:    12038.99 [#/sec] (mean)
Time per request:       5.316 [ms] (mean)
Time per request:       0.083 [ms] (mean, across all concurrent requests)
Transfer rate:          7089.37 [Kbytes/sec] received
```
