/**
Concept of PSQL data type. Dynamic (runtime) typing system.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.type;

import dpeq.constants;
import dpeq.marshalling;



/// Storage for types, known to this process
shared PGType[ObjectID] s_pgTypes;

/// Convenience name->PgType map
shared PGType[string] s_pgTypesBn;

/// Add type to global type registry
void register_pgtype(shared PGType type)
{
    // be careful with the threading. You should probably call this
    // function only on the start of your application, right after initial
    // connect to DB.
    s_pgTypes[type.typeId] = type;
    s_pgTypesBn[type.name] = type;
}

/// Query global type registry
shared(PGType) pgType(ObjectID oid) @safe
{
    return s_pgTypes[oid];
}
/// ditto
shared(PGType) pgType(string name) @safe
{
    return s_pgTypesBn[name];
}


/// Abstract PSQL type for dynamic type binding
abstract class PGType
{
    ObjectID typeId;

    /// for example, 'bigint'
    string name;

    /// is this type binary-transferred?
    FormatCode formatCode;

    /** Write `what` into byte buffer `to`, with respect to Network byte order
    * (most significant byte first). Should return number of bytes written.
    * Return -2 if the buffer is too small. Don't throw please. */
    abstract int marshal(ubyte[] to, void* what) const nothrow;
}
