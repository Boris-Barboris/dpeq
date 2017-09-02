// All tests live here.
// They are examples as well.

import dpeq;

import std.stdio;


void main()
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
