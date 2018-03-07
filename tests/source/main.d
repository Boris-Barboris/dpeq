/**
Simple tests that serve as an example.

Copyright: Copyright Boris-Barboris 2017-2018.
License: MIT
Authors: Boris-Barboris
*/

import dpeq;

import std.array: join, array;
import std.conv: to;
import std.typecons: Nullable, scoped;
import std.meta;
import std.stdio;
import std.variant: visit;
import std.uuid: UUID, randomUUID;

import core.thread;
import core.time;


/// test spec of a table row, wich includes the specializations of a
/// StaticFieldMarshaller, as well as a couple of types wich are supposed to
/// fallback to PromiscuousStringMarshaller
enum FieldSpec[] testTableSpec = [
    FieldSpec(PgType.BOOLEAN, false),
    FieldSpec(PgType.BOOLEAN, true),
    FieldSpec(PgType.BIGINT, false),
    FieldSpec(PgType.BIGINT, true),
    FieldSpec(PgType.SMALLINT, false),
    FieldSpec(PgType.SMALLINT, true),
    FieldSpec(PgType.INT, false),
    FieldSpec(PgType.INT, true),
    FieldSpec(PgType.VARCHAR, false),
    FieldSpec(PgType.VARCHAR, true),
    FieldSpec(PgType.TEXT, false),
    FieldSpec(PgType.TEXT, true),
    FieldSpec(PgType.UUID, false),
    FieldSpec(PgType.UUID, true),
    FieldSpec(PgType.REAL, false),
    FieldSpec(PgType.REAL, true),
    FieldSpec(PgType.DOUBLE, false),
    FieldSpec(PgType.DOUBLE, true),
    // This PgTypes are not handled by StaticFieldMarshaller and should fallback
    // to string representation.
    FieldSpec(PgType.INET, false),
    FieldSpec(PgType.INET, true)
];

enum FormatCode[] testTableRowFormats = FSpecsToFCodes!(testTableSpec);

alias TestTupleT = TupleForSpec!testTableSpec;

string createTableCommand()
{
    string res = "CREATE TABLE dpeq_test (";
    string[] colDefs;
    foreach (i, col; aliasSeqOf!testTableSpec)
    {
        colDefs ~= "col" ~ i.to!string ~ " " ~
            col.typeId.pgTypeName ~ (col.nullable ? "" : " NOT NULL");
    }
    res ~= colDefs.join(", ") ~ ");";
    writeln("table will be created with query: ", res);
    return res;
}

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
    return res;
}

alias ConT = PSQLConnection!(StdSocket);

void createTestSchema(ConT)(ConT con)
{
    con.postSimpleQuery(createTableCommand());
    con.flush();
    con.getQueryResults();
}

void main()
{
    auto con = new ConT(
        BackendParams("127.0.0.1", cast(ushort)5432, "postgres", "r00tme", "dpeqtestdb"));
    createTestSchema(con);
    auto ps = new PreparedStatement!ConT(con, insertCommand(),
        testTableSpec.length, null, false);
    auto portal = new Portal!ConT(ps, false);
    ps.postParseMessage();

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
        "192.168.0.1",
        Nullable!string("127.0.0.1")
    );

    portal.bind!(testTableSpec, testTableRowFormats)(sentTuple.expand);
    portal.execute();

    con.sync();
    con.flush();
    auto res = con.getQueryResults();
    assert(res.blocks.length == 1);

    FieldDescription[] rowDesc = res.blocks[0].rowDesc[].array;
    writeln("received field descriptions:");
    foreach (vald; rowDesc)
    {
        writeln(["name: " ~ vald.name, "type: " ~ vald.type.to!string,
            "format code: " ~ vald.formatCode.to!string].join(", "));
    }

    // convert result to tuples
    auto rows = blockToTuples!testTableSpec(res.blocks[0].dataRows);
    foreach (row; rows)
    {
        import std.range: iota;
        writeln("\nrow received, it's tuple representation:");
        foreach (i; aliasSeqOf!(iota(0, testTableSpec.length).array))
        {
            writeln(rowDesc[i].name, " = ", row[i]);
        }
        assert(row == sentTuple, "Sent and recieved tuples don't match");
    }

    // convert result to variants
    auto variantRows = blockToVariants(res.blocks[0]);
    foreach (row; variantRows)
    {
        writeln("\nrow received, it's variant representation:");
        foreach (col; row)
            writeln(col.type, " ", col.toString);
    }

    // close connection
    con.terminate();


    // other tests:
    transactionExample();
    notifyExample();
    version(Posix) unixSocketExample();
}



/// example wich demonstrates implicit transaction scope of EQ prtocol
void transactionExample()
{
    void threadFunc1()
    {
        auto con = new ConT(
            BackendParams("127.0.0.1", cast(ushort)5432, "postgres",
            "r00tme", "dpeqtestdb"));
        // parse and bind and execute for SELECT FOR UPDATE
        {
            // unnamed prepared statement
            auto ps = scoped!(PreparedStatement!ConT)(con,
                "SELECT * FROM dpeq_test FOR UPDATE;", cast(short) 0);
            ps.parse();
            // unnamed portal
            auto pt = scoped!(Portal!ConT)(ps, false);
            pt.bind();
            pt.execute(false);
            // since we don't send sync (wich would close the implicit transaction),
            // backend will not return result of this select until we request it
            // via Flush message
            con.putFlushMessage();
            con.flush();
        }
        Thread.sleep(seconds(2));   // sleep in order to demonstrate row locking
        // we now update all (there is only one) row
        {
            // unnamed prepared statement
            auto ps = scoped!(PreparedStatement!ConT)(con,
                "UPDATE dpeq_test SET col0 = 't';", cast(short) 0);
            ps.parse();
            // unnamed portal
            auto pt = scoped!(Portal!ConT)(ps, false);
            pt.bind();
            pt.execute(false);
            // this Sync effectively commits and drops the lock on table row
            con.sync();
            con.flush();
        }
        getQueryResults(con);
        con.terminate();
    }


    void threadFunc2()
    {
        Thread.sleep(msecs(500));   // let first thread aquire lock
        auto con = new ConT(
            BackendParams("127.0.0.1", cast(ushort)5432, "postgres",
            "r00tme", "dpeqtestdb"));

        // simple select to ensure old false value
        {
            // unnamed prepared statement
            auto ps = scoped!(PreparedStatement!ConT)(con,
                "SELECT * FROM dpeq_test;", cast(short) 0);
            ps.parse();
            // unnamed portal
            auto pt = scoped!(Portal!ConT)(ps, false);
            pt.bind(testTableRowFormats);
            pt.execute(false);
            con.sync();
            con.flush();
        }

        auto res = getQueryResults(con);
        auto rows = blockToTuples!testTableSpec(res.blocks[0].dataRows);
        assert(rows.front[0] == false, "Unexpected 'true' in first column");

        // parse and bind and execute for SELECT FOR UPDATE
        {
            // unnamed prepared statement
            auto ps = scoped!(PreparedStatement!ConT)(con,
                "SELECT * FROM dpeq_test FOR UPDATE;", cast(short) 0);
            ps.parse();
            // unnamed portal
            auto pt = scoped!(Portal!ConT)(ps, false);
            pt.bind(testTableRowFormats);
            pt.execute(false);
            con.sync();     // releases row lock of thread 2
            con.flush();
        }

        // this returns approx after 2 seconds, right after first thread commits
        // and releases row locks
        res = getQueryResults(con);
        rows = blockToTuples!testTableSpec(res.blocks[0].dataRows);
        assert(rows.front[0] == true,
            "First thread's commit is not visible in second thread");
        con.terminate();
    }

    auto thread1 = new Thread(&threadFunc1).start();
    auto thread2 = new Thread(&threadFunc2).start();

    thread1.join();
    thread2.join();
}




/// example wich demonstrates PSQL notify
void notifyExample()
{
    void threadFunc1()
    {
        auto con = new ConT(
            BackendParams("127.0.0.1", cast(ushort)5432, "postgres",
            "r00tme", "dpeqtestdb"));
        Thread.sleep(msecs(500));   // make sure second thread has connected
        con.postSimpleQuery("NOTIFY chan1, 'Payload1337';");
        con.flush();
        con.pollMessages(null);
        con.terminate();
    }

    void threadFunc2()
    {
        auto con = new ConT(
            BackendParams("127.0.0.1", cast(ushort)5432, "postgres",
            "r00tme", "dpeqtestdb"));
        Notification inbox;
        con.notificationCallback = (Notification n) { inbox = n; return true; };
        con.postSimpleQuery("LISTEN chan1;");
        con.flush();
        con.pollMessages(null);
        // blocks for approx half a second
        con.pollMessages(null);
        con.terminate();
        assert(inbox.channel == "chan1");
        assert(inbox.payload == "Payload1337");
    }

    auto thread1 = new Thread(&threadFunc1).start();
    auto thread2 = new Thread(&threadFunc2).start();

    thread1.join();
    thread2.join();
}



version(Posix)
{
    /// example wich demonstrates connection through Unix-domain socket
    void unixSocketExample()
    {
        // Default BackendParams.host is set to
        // /var/run/postgresql/.s.PGSQL.5432
        auto con = new ConT(BackendParams());
        con.postSimpleQuery("select version();");
        con.flush();
        auto res = con.getQueryResults();
        auto firstRow = blockToVariants(res.blocks[0])[0];
        writeln("psql version: ", firstRow.front);
    }
}