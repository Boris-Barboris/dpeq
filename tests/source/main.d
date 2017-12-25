// All tests live here.
// They are examples as well.

import dpeq;

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

void main()
{
    bind_example();
    //tuple_select();
    //select_example();
}

void bind_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    auto ps = new PreparedStatement!(typeof(con))
        (con, "SELECT * FROM sessions LIMIT $1;", 1, null, true);
    auto portal = new Portal!(typeof(con))(ps, true);
    ps.postParseMessage();
    portal.bind!([FieldSpec(StaticPgTypes.BIGINT, false)], fullRowFormats)(3);
    portal.execute(false);
    con.sync();
    con.flush();
    ps.ensureParseComplete();
    writefln("Parse complete");
    portal.ensureBindComplete();
    writefln("Bind complete");

    auto res = con.getQueryResults();
    auto rows = blockToTuples!SessionSpec(res.blocks[0].dataRows);
    foreach (row; rows)
    {
        writeln("iterating over data row");
        writeln("row = ", row);
    }

    portal.postCloseMessage();
    ps.postCloseMessage();
    con.sync();
    con.terminate();
}

void tuple_select()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    con.postSimpleQuery("SELECT * FROM sessions LIMIT 2;");
    con.flush();
    auto res = con.getQueryResults();
    writeln("Got result ", res);
    auto rows = blockToTuples!SessionSpec(res.blocks[0]);
    foreach (row; rows)
    {
        writeln("iterating over data row");
        writeln("row = ", row);
    }
    con.terminate();
}

void transaction_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
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

void select_example()
{
    auto con = new PSQLConnection!(StdSocket, writefln, writefln)(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    con.postSimpleQuery("SELECT * FROM sessions LIMIT 2;");
    con.flush();
    auto res = con.getQueryResults();
    writeln("Got result ", res);
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
