// All tests live here.
// They are examples as well.

import dpeq;

import std.stdio;


void main()
{
    tuple_select();
}

void tuple_select()
{
    blockToTuples!([
        FieldSpec(StaticPgTypes.BOOLEAN, true),
        FieldSpec(StaticPgTypes.SMALLINT, false)])();
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
