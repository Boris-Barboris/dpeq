/**
Simple tests that serve as an example.

Copyright: Copyright Boris-Barboris 2017-2018.
License: MIT
Authors: Boris-Barboris
*/

import dpeq;

import std.array: join;
import std.conv: to;
import std.typecons: Nullable;
import std.meta;
import std.stdio;
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

template FormatCodeFromFieldSpec(FieldSpec spec)
{
    enum FormatCodeFromFieldSpec =
        DefaultFieldMarshaller!(spec).formatCode;
}

enum FormatCode[] testTableRowFormats =
    [staticMap!(FormatCodeFromFieldSpec, aliasSeqOf!testTableSpec)];

string createTableCommand()
{
    string res = "CREATE TABLE dpeq_test (";
    string[] colDefs;
    foreach (i, col; aliasSeqOf!testTableSpec)
    {
        colDefs ~= "col" ~ i.to!string ~ " " ~ col.typeId.to!StaticPgTypes.to!string;
        static if (!col.nullable)
            colDefs ~= " NOT NULL";
    }
    res ~= colDefs.join(", ") ~ ");";
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
    res ~= colDefs.join(", ") ~ ") RETURNING *;";
    return res;
}

void main()
{
    alias ConT = PSQLConnection!(StdSocket);
    auto con = new ConT(
        BackendParams("127.0.0.1", cast(ushort)5432, "postgres", "", "dpeqtestdb"));
    createTestSchema(con);
    auto ps = new PreparedStatement!ConT(con, insertCommand(), testTableSpec.length, null, false);
    auto portal = new Portal!ConT(ps, false);
    ps.postParseMessage();
    portal.bind!(testTableSpec, testTableRowFormats)(
        false,
        Nullable!bool(true),
        123L,
        Nullable!long(333333L),
        cast(short) 3,
        Nullable!short(4),
        6,
        Nullable!int(-3),
        "123",
        Nullable!string(),
        "asdjaofdfad",
        Nullable!string("12393"),
        randomUUID(),
        Nullable!UUID(randomUUID()),
        3.14f,
        Nullable!float(float.nan),
        -3.14,
        Nullable!double(),
        "192.168.0.1",
        Nullable!string("127.0.0.1")
    );
    portal.execute();
    con.sync();
    con.flush();
    auto res = con.getQueryResults();
    assert(res.blocks.length == 1);
    // convert result to tuples
    auto rows = blockToTuples!testTableSpec(res.blocks[0].dataRows);
    foreach (row; rows)
    {
        pragma(msg, "tuple-row type ", typeof(row));
        writeln("row = ", row);
    }
    // convert result to variants
    auto variantRows = blockToVariants(res.blocks[0]);
    foreach (row; variantRows)
    {
        writeln("iterating over data row");
        foreach (col; row)
            writeln("column ", col);
    }
}

void createTestSchema(ConT)(ConT con)
{
    con.postSimpleQuery(createTableCommand());
    con.flush();
    con.getQueryResults();
}