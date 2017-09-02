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

/// Simple query always returns data in TEXT format.
void postSimpleQuery(ConnT)(ConnT conn, string query)
{
    conn.putQueryMessage(query);
}


/////////////////////////////////////
// Methods to get query results
/////////////////////////////////////

/// The most generic dynamic method, suitable for
/// non-typed apriori-unknown queries.
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
