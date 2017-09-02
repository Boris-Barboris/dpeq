/**
Commands of various nature.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.command;

import std.exception: enforce;
import std.conv: to;
import std.traits;
import std.range;

import dpeq.exceptions;
import dpeq.connection;
import dpeq.constants;
import dpeq.marshalling;
import dpeq.schema;


/////////////////////////////////////
// Different forms of command input
/////////////////////////////////////

/// Simple query is simple. Sent string to server and get responses.
/// The most versatile, unsafe way to issue commands to PSQL. It is also slow.
/// Simple query always returns data in FormatCode.Text format.
void postSimpleQuery(ConnT)(ConnT conn, string query)
{
    conn.putQueryMessage(query);
}


/////////////////////////////////////
// Functions to get query results
/////////////////////////////////////

/// Generic dynamic method, suitable for both simple and prepared queries.
QueryResult getQueryResults(ConnT)(ConnT conn)
{
    QueryResult res;

    bool interceptor(Message msg, ref bool err, ref string errMsg)
    {
        with (BackendMessageType)
        switch (msg.type)
        {
            case EmptyQueryResponse:
                res.empty = true;
                break;
            case CommandComplete:
                res.commandsComplete++;
                break;
            case RowDescription:
                RowBlock rb;
                rb.rowDesc = dpeq.schema.RowDescription(msg.data);
                res.blocks ~= rb;
                break;
            case DataRow:
                if (res.blocks.length == 0)
                {
                    err = true;
                    errMsg ~= "Got row without row description ";
                }
                else
                    res.blocks[$-1].dataRows ~= msg; // we simply save raw bytes
                break;
            default:
                break;
        }
        return false;
    }

    conn.pollMessages(&interceptor, false);
    return res;
}
