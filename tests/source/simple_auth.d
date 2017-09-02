module dpeq_tests.simple_auth;

import dpeq;

void main()
{
    PSQLConnection con = new PSQLConnection(
        BackendParams("localhost", cast(ushort)5432, "postgres", "r00tme", "drova"));
    con.terminate();
}
