# DPEQ - native PSQL extended query protocol client for D programming language

[![Build Status](https://travis-ci.org/Boris-Barboris/dpeq.svg?branch=master)](https://travis-ci.org/Boris-Barboris/dpeq)

**dpeq** is a source library that implements client side of PostgreSQL 
extended query (EQ) protocol. EQ is a stateful message-based binary protocol 
working on top of TCP\IP or Unix-domain sockets. **dpeq** defines classes 
to hold the required state and utility functions, that send and recieve protocol 
messages in sensible manner. On top of that, dpeq includes extensible 
schema-oriented marshalling functionality, wich maps PSQL types to their 
binary or text representations, native to D.   

Here is a list of good links to get yourself familiar with EQ protocol, wich may
help you to understand the nature of the messages being passed:   
https://www.postgresql.org/docs/9.5/static/protocol.html   
https://www.postgresql.org/docs/9.5/static/protocol-flow.html   
https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html   

Many thanks to authors of https://github.com/pszturmaj/ddb and
https://github.com/teamhackback/hb-ddb, wich gave this library inspiration.

## Source structure
* source/dpeq/connection.d - buffered message streamer *PSQLConnection* is used
  to send and recieve messages. It's a class template with customazable socket
  type (so you can easily use it with vibe-d) and logging functions.
* source/dpeq/schema.d - structures that describe query results.
* source/dpeq/command.d - classes and functions that implement typical operations
  on the connection. You can find examples of *PreparedStatement* and *Portal* 
  class implementations there. *getQueryResults* function
  from this module is bread and butter for response demarshalling. *blockToVariants*
  and *blockToTuples* are example demarshalling implementations that allow you to
  lazily work with QueryResults, returned by *getQueryResults*.
* source/dpeq/constants.d - sorry ass header file.
* source/dpeq/exceptions.d - exceptions, explicitly thrown from dpeq code.
* source/dpeq/marshalling.d - templated (de)marshalling code, used throughout
  dpeq. This is the place to research for ways to inject custom type behaviour.

## How to use vibe-d sockets?
Wrap them into class and pass it as a template parameter to PSQLConnection.
```D
final class SocketT
{
    // Vibe-d TCPConnection
    TCPConnection m_con;

    this(string host, ushort port)
    {
        m_con = connectTCP(host, port);
        // maybe set timeouts and keepalive here
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

## Supported native types
SMALLINT, INT, OID, BIGINT, BOOLEAN, UUID, REAL, DOUBLE PRECISION 
are handled using their respective binary type representations. Types that are 
unknown to the marshalling template are transferred using their text representation, 
thus delegating additional parsing and validation check to PostgreSQL server.
To quickly hack missing types in, *DefaultFieldMarshaller*, *VariantConverter* 
and most marshalling-related templates accept template parameters wich can be
used to override or extend type mapping from the client code.

## Supported authorization mechanisms
Only trusted (trivial), password and md5 are implemented.

## Detailed description of the test example
*tests/source/main.d* contains a test that demonstrates the library usage. This
section will try to explain in detail, what is happening in the code.

### Establish the connection
```D
    /* Dpeq source is heavily templated. Most functions and classes accept one or
    more template parameters that customize their behaviour. It is convenient
    to define a number of aliases that instantiate those templates and shorten
    definitions. 
    
    This aliases ConT to dpeq connection that uses Phobos TCP sockets*/
    alias ConT = PSQLConnection!(StdSocket);    // 

    /* PSQLConnection constructor does the following:
        - allocate internally used storage for write buffer
        - construct the socket object, wich should establish duplex connection between
            your program and the database. This may throw.
        - initializes EQ connection by sending required handshake and authorization
            messages. Repeatedly reads the socket until the handshake succeeds or
            fails.
    If the constructor succeeds, connection is opened and ready to be used. */
    auto con = new ConT(
        BackendParams("127.0.0.1", cast(ushort)5432, "postgres", "r00tme", "dpeqtestdb"));
    // when you want to close the connection, call...
    con.terminate();    // will swallow all exceptions
```
### Create table using *simpleQuery*
```D
/* 
Most marshalling functions in dpeq are statically typed. Remote, postgresql
types are treated as an absolute truth, not the other way around. Dpeq templates
then perform lookup and validate types you pass to dpeq functions during
compilation. Although PSQLConnection and Portal interfaces (bind-related
calls are generic) are flexible enough to use them with runtime-dispatched, OOP
values, dpeq only implements static lookup. 

You can think of it this way: values passed to the socket can be represented by 
statically known tuple, or by a range of interfaces that implement marshalling 
methods. Ultimately, PSQLConnection accepts the second type, however the rest
of the library wrap it in the first type.

    This FieldSpec array defines one specific tuple. */
enum FieldSpec[] testTableSpec = [
    FieldSpec(PgType.BOOLEAN, false),   // bool
    FieldSpec(PgType.BOOLEAN, true),    // Nullable!bool
    FieldSpec(PgType.BIGINT, false),    // long
    FieldSpec(PgType.BIGINT, true),     // Nullable!long
    FieldSpec(PgType.SMALLINT, false),  // ...
    FieldSpec(PgType.SMALLINT, true),
    FieldSpec(PgType.INT, false),
    FieldSpec(PgType.INT, true),
    FieldSpec(PgType.VARCHAR, false),   // string
    FieldSpec(PgType.VARCHAR, true),    // Nullable!string
    FieldSpec(PgType.TEXT, false),      // string as well
    FieldSpec(PgType.TEXT, true),       // Nullable!string
    FieldSpec(PgType.UUID, false),      // std.uuid.UUID
    FieldSpec(PgType.UUID, true),
    FieldSpec(PgType.REAL, false),      // float
    FieldSpec(PgType.REAL, true),
    FieldSpec(PgType.DOUBLE, false),    // double
    FieldSpec(PgType.DOUBLE, true),
    // This PgTypes are not handled by StaticFieldMarshaller and should fallback
    // to string representation.
    FieldSpec(PgType.INET, false),      // string
    FieldSpec(PgType.INET, true)        // Nullable!string
];

// This function build a SQL query wich creates our test table.
string createTableCommand()
{
    string res = "CREATE TABLE dpeq_test (";
    string[] colDefs;
    foreach (i, col; aliasSeqOf!testTableSpec)
    {
        colDefs ~= "col" ~ i.to!string ~ " " ~ 
            col.typeId.pgTypeName ~ (col.nullable ? "" : " NOT NULL");
    }
    res ~= colDefs.join(", ") ~ ");";   // std.array.join
    writeln("table will be created with query: ", res);
    /* prints:
    table will be created with query: CREATE TABLE dpeq_test (col0 BOOLEAN 
    NOT NULL, col1 BOOLEAN, col2 BIGINT NOT NULL, col3 BIGINT, col4 SMALLINT 
    NOT NULL, col5 SMALLINT, col6 INT NOT NULL, col7 INT, col8 VARCHAR NOT NULL, 
    col9 VARCHAR, col10 TEXT NOT NULL, col11 TEXT, col12 UUID NOT NULL, 
    col13 UUID, col14 REAL NOT NULL, col15 REAL, col16 double precision 
    NOT NULL, col17 double precision, col18 INET NOT NULL, col19 INET);
    */
    return res;
}

void createTestSchema(ConT)(ConT con)
{
    /*
    postSimpleQuery is related to EQ's predecessor, simple query
    protocol. It is a text-only message format, wich is well suited for
    parameterless, reliable (no user input) queries.

    postSimpleQuery takes our "CREATE TABLE..." sql query and writes it to
    con's write buffer. 
    */
    con.postSimpleQuery(createTableCommand());
    
    /*
    flush() actually sends connection's write buffer to the socket. 
    PSQLConnection maintains it's own write buffer in order to
    increase the efficiency. Lesser segment fragmentation and syscall frequency
    are an obvious advantage. You can also protect the server from reading junk
    when the last message from a group of logically grouped (transaction) has
    failed to marshal. You can return an error to user and clear the write buffer
    without bothering the server (PSQLConnection.discard method).
    */
    con.flush();

    /*
    getQueryResult calls PSQLConnection.pollMessages wich repeatedly reads 
    messages from the socket and fills QueryResult structure with the raw data.
    Polling stops when ErrorResponse or ReadyForQuery messages are recieved, or
    the socket throws. Error then is rethrown.

    QueryResult is a blob of unmarshalled messages. Usually, it contains
    contents of RowDescription message and arrays of actual data rows.

    Every postSimpleQuery or PSQLConnection.sync MUST be accompanied by 
    getQueryResults call. Generally, you should be very careful with
    ReadyForQuery messages. Also, take a look at PSQLConnection.delayedPoll
    method.
    */
    con.getQueryResults();
}
```
### Insert example using prepared statement and a portal
```D

/* 
FSpecsToFCodes converts array of FieldSpecs to the array of FormatCodes.
EQ protocol requires client to explicitly specify return type format codes, if
the client wants to use binary data transfer. If not, all values in responses
will be transferred as text. All demarshallers defined in dpeq accept text
representation.

This line evaluates the efficient array of format codes for the testTableSpec
tuple. Everything the DefaultFieldMarshaller will accept in binary will be
requested in binary.
*/
enum FormatCode[] testTableRowFormats = FSpecsToFCodes!(testTableSpec);

/*
This function, like a function createTableCommand() in a previous example,
creates an SQL statement;
*/
string insertCommand()
{
    string res = "INSERT INTO dpeq_test (";
    string[] colDefs;
    foreach (i, col; aliasSeqOf!testTableSpec)
        colDefs ~= "col" ~ i.to!string;
    res ~= colDefs.join(", ") ~ ") VALUES (";
    string[] parDefs;
    foreach (i, col; aliasSeqOf!testTableSpec)
        parDefs ~= "$" ~ (i + 1).to!string;
    res ~= parDefs.join(", ") ~ ") RETURNING *;";
    writeln("insert will be ran with query: ", res);
    /* prints:
    insert will be ran with query: INSERT INTO dpeq_test (col0, col1, col2, 
    col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, 
    col14, col15, col16, col17, col18, col19) VALUES ($1, $2, $3, $4, $5, $6, 
    $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20) RETURNING *;
    */
    return res;
}

// TestTupleT is aliased to tuple, returned by dpeq
alias TestTupleT = TupleForSpec!testTableSpec;

void main()
{
    // ...

    /*
    Prepared statement is a class that represents remote prepared statement from
    the EQ protocol. Prepared statement is a pre-parsed and semantically checked
    SQL query. Prepared statement may be named and unnamed:
        - named are persistent and require explicit closing in order to be reparsed
        - unnamed is volatile and is easily reparsed.
        - persist flag in PreparedStatement constructor controls the type of
            the prepared statement.
    Fourth argument is an optional array of explicitly stated parameter types. 
    I believe it is an alternative to PSQL ::<type> syntax for explicit casting,
    but I am not sure exactly when to use it. In my experience, you can simply
    leave the array empty.

    Relevant quote:
    "...If successfully created, a named prepared-statement object lasts till the 
    end of the current session, unless explicitly destroyed. An unnamed prepared
    statement lasts only until the next Parse statement specifying the unnamed 
    statement as destination is issued. (Note that a simple Query message also
    destroys the unnamed statement.) Named prepared statements must be 
    explicitly closed before they can be redefined by another Parse message, 
    but this is not required for the unnamed statement."
    */
    auto ps = new PreparedStatement!ConT(con, insertCommand(), 
        testTableSpec.length, null, false);

    /*
    Portals represent parameter values, bound to particular prepared statement.
    Just like prepared statement, they can be named and unnamed.

    Relevant quote:
    "Once a prepared statement exists, it can be readied for execution using a 
    Bind message. The Bind message gives the name of the source prepared statement 
    (empty string denotes the unnamed prepared statement), the name of the 
    destination portal (empty string denotes the unnamed portal), and the values 
    to use for any parameter placeholders present in the prepared statement... 
    Bind also specifies the format to use for any data returned by the query; 
    the format can be specified overall, or per-column...

    If successfully created, a named portal object lasts till the end of the 
    current transaction, unless explicitly destroyed. An unnamed portal is 
    destroyed at the end of the transaction, or as soon as the next Bind 
    statement specifying the unnamed portal as destination is issued. 
    (Note that a simple Query message also destroys the unnamed portal.) 
    Named portals must be explicitly closed before they can be redefined by 
    another Bind message, but this is not required for the unnamed portal."
    */
    auto portal = new Portal!ConT(ps, false);

    ps.postParseMessage();  // puts Parse message to connection write buffer

    // Here we simply build test tuple wich we will insert into postgres table
    TestTupleT sentTuple = TestTupleT(
        false,
        Nullable!bool(true),
        123L,
        Nullable!long(333333L),
        cast(short) 3,
        Nullable!short(4),
        6,
        Nullable!int(-3),
        "123",
        Nullable!string(),  // null
        "asdjaofdfad",
        Nullable!string("12393"),
        randomUUID(),
        Nullable!UUID(randomUUID()),
        3.14f,
        Nullable!float(float.infinity),
        -3.14,
        Nullable!double(),  // null
        // notice, that PgType.INET , wich is unknown to binary 
        // marshalling templates, is represented by string in TestTupleT.
        "192.168.0.1",
        Nullable!string("127.0.0.1")
    );

    /*
    Portal class implements bind method, wich:
        - type-checks arguments wich row spec.
        - closes previous portal if it is a persistent one.
        - builds the array of scoped delegates, pointing to correct marshallers
          to pass it to PSQLConnection.putBindMessage method.
        - calls PSQLConnection.putBindMessage, wich orderly calls marshallers
          and fills connection's write buffer.
        - rolls write buffer back if marshalling fails.
    */
    portal.bind!(testTableSpec, testTableRowFormats)(sentTuple.expand);

    /*
    Portal.execute puts the Execute message into the write buffer. On recieving,
    PSQL fires the query plan and starts pushing results to it's 
    end of the socket.

    Optional parameter 'describe', when set to false, prevents the server from
    sending RowDescription message, thus saving the network capacity. Some
    demarshalling functions require the QueryResult to have RowDescription.
    One of the 'blockToTuples' overloads does not, and is therefore recommended
    for folks of all ages.

    quote:
    "Query planning typically occurs when the Bind message is processed. If 
    the prepared statement has no parameters, or is executed repeatedly, the 
    server might save the created plan and re-use it during subsequent Bind 
    messages for the same prepared statement. However, it will do so only if 
    it finds that a generic plan can be created that is not much less efficient 
    than a plan that depends on the specific parameter values supplied...
      Once a portal exists, it can be executed using an Execute message. 
    The Execute message specifies the portal name (empty string denotes the 
    unnamed portal)..."
    */
    portal.execute();

    /*
    Sync message is a synchronization mechanism. The best way to describe,
    why is it needed, is to quote the docs:
    
    "At completion of each series of extended-query messages, the frontend 
    should issue a Sync message. This parameterless message causes the backend 
    to close the current transaction if it's not inside a BEGIN/COMMIT 
    transaction block ("close" meaning to commit if no error, or roll back 
    if error). Then a ReadyForQuery response is issued. The purpose of Sync is 
    to provide a resynchronization point for error recovery. When an error is
     detected while processing any extended-query message, the backend issues 
     ErrorResponse, then reads and discards messages until a Sync is reached, 
     then issues ReadyForQuery and returns to normal message processing. (But 
     note that no skipping occurs if an error is detected while processing Sync 
     — this ensures that there is one and only one ReadyForQuery sent for 
     each Sync.)"

     TLDR: call sync() before each getQueryResults, IF you are using EQ. Sync
     will produce duplicated ReadyForQuery message when used together with
     Simple Query protocol message, breaking your next getQueryResults.
    */
    con.sync();
    
    con.flush();    // flush write buffer to socket
    
    // allocate RAM for reponse and return it's raw representation
    auto res = con.getQueryResults();
    assert(res.blocks.length == 1);
    // ...
}
```
### Working with QueryResult
```D
    // ...
    auto res = con.getQueryResults();

    /*
    Row descriptions in result blocks are treated as HTTP headers -
    rarely needed hence lazily demarshalled. rowDesc should be sliced ([])
    in order to get an InputRange of FieldDescription structures, each 
    describing it's own column. The line below eagerly allocates an array
    in order to support random access, and fills it with partially-demarshalled
    FieldDescriptions.
    */
    FieldDescription[] rowDesc = res.blocks[0].rowDesc[].array;
    writeln("received field descriptions:");
    foreach (vald; rowDesc)
    {
        /*
        formatCode property actually calls the number conversion
        code (bigendian to x86 little-endian) wich gets you a nice native
        number to read. Strings, like in most demarshalling functions,
        are not reallocated and span the memory of the original message,
        received from the socket.
        */
        writeln(["name: " ~ vald.name, "type: " ~ vald.type.to!string, 
            "format code: " ~ vald.formatCode.to!string].join(", "));
        /* prints:
        received field descriptions:
        name: col0, type: 16, format code: Binary
        name: col1, type: 16, format code: Binary
        name: col2, type: 20, format code: Binary
        name: col3, type: 20, format code: Binary
        name: col4, type: 21, format code: Binary
        name: col5, type: 21, format code: Binary
        name: col6, type: 23, format code: Binary
        name: col7, type: 23, format code: Binary
        name: col8, type: 1043, format code: Text
        name: col9, type: 1043, format code: Text
        name: col10, type: 25, format code: Text
        name: col11, type: 25, format code: Text
        name: col12, type: 2950, format code: Binary
        name: col13, type: 2950, format code: Binary
        name: col14, type: 700, format code: Binary
        name: col15, type: 700, format code: Binary
        name: col16, type: 701, format code: Binary
        name: col17, type: 701, format code: Binary
        name: col18, type: 869, format code: Text
        name: col19, type: 869, format code: Text
        */
    }

    /*
    This overload of blockToTuples takes in an array of data messages that
    belong to one data block and converts them to random access range of
    lazily-demarshalled tuples. We expect exactly the same tuple we have
    inserted, hence the usage of testTableSpec as an expected row spec.
    */
    auto rows = blockToTuples!testTableSpec(res.blocks[0].dataRows);
    foreach (row; rows) // actual call to demarshallers happens here
    {
        import std.range: iota;
        writeln("\nrow received, it's tuple representation:");
        foreach (i; aliasSeqOf!(iota(0, testTableSpec.length).array))
        {
            writeln(rowDesc[i].name, " = ", row[i]);
        }
        assert(row == sentTuple, "Sent and recieved tuples don't match");
        /* prints:
        row received, it's tuple representation:
        col0 = false
        col1 = true
        col2 = 123
        col3 = 333333
        col4 = 3
        col5 = 4
        col6 = 6
        col7 = -3
        col8 = 123
        col9 = Nullable.null
        col10 = asdjaofdfad
        col11 = 12393
        col12 = 266f36a2-acac-4eb0-8cc3-24907b886f6e
        col13 = 36771050-164b-4493-9372-860bbef83ef8
        col14 = 3.14
        col15 = nan
        col16 = -3.14
        col17 = Nullable.null
        col18 = 192.168.0.1
        col19 = 127.0.0.1
        */
    }

    /*
    Alternative to the tuple converter is a variant converter, wich
    looks onto row description and deduces the type of demarshaller
    dynamically. It returns RandomAccessRange of InputRanges of 
    lazily-demarshalled variants. 
    By default it's the subtype of std.variant.Variant, wich has convenient
    isNull function defined to keep it in line with Nullable interface.
    */
    auto variantRows = blockToVariants(res.blocks[0]);
    foreach (row; variantRows)
    {
        writeln("\nrow received, it's variant representation:");
        foreach (col; row)  // actual call to demarshallers happens here
            writeln(col.type, " ", col.toString);
        /* prints:
        row received, it's variant representation:
        bool false
        bool true
        long 123
        long 333333
        short 3
        short 4
        int 6
        int -3
        immutable(char)[] 123
        void null
        immutable(char)[] asdjaofdfad
        immutable(char)[] 12393
        std.uuid.UUID 266f36a2-acac-4eb0-8cc3-24907b886f6e
        std.uuid.UUID 36771050-164b-4493-9372-860bbef83ef8
        float 3.14
        float nan
        double -3.14
        void null
        immutable(char)[] 192.168.0.1
        immutable(char)[] 127.0.0.1
        */
    }
```