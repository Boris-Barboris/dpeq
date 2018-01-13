/**
Simple tests that serve as an example.

Copyright: Copyright Boris-Barboris 2017-2018.
License: MIT
Authors: Boris-Barboris
*/

import dpeq;

import std.array: join, array;
import std.conv: to;
import std.typecons: Nullable;
import std.meta;
import std.stdio;
import std.variant: visit;
import std.uuid: UUID, randomUUID;


// test spec of a table row, wich includes the specializations of a
// StaticFieldMarshaller, as well as a couple of types wich are supposed to
// fallback to PromiscuousStringMarshaller
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

void main()
{
    alias ConT = PSQLConnection!(StdSocket);
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
        writeln("\nrow received, it's tuple representation:");
        foreach (i, col; aliasSeqOf!testTableSpec)
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
}

void createTestSchema(ConT)(ConT con)
{
    con.postSimpleQuery(createTableCommand());
    con.flush();
    con.getQueryResults();
}