/**
Primitives used for marshalling.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.marshalling;

import std.algorithm: canFind;
import std.bitmanip: nativeToBigEndian;
import std.conv: to;
import std.traits;

import dpeq.constants;


/// Query parameter descriptor
struct ParamT
{
    ObjectID type;
    bool nullable;
}


/////////////////////////////////
// Marshalling
/////////////////////////////////


/** Default compile-time oriented marshaller.
* You can extend it with two custom marshallers: Pre and Post. */
template DefaultMarshaller(ParamT type, alias Pre = NopMarshaller,
    alias Post = NopMarshaller)
{
    static if (Pre!type.canDigest)
    {
        enum formatCode = Pre!type.formatCode;
        alias marshal = Pre!type.marshal;
    }
    else static if (StaticMarshaller!type.canDigest)
    {
        enum formatCode = StaticMarshaller!type.formatCode;
        alias marshal = StaticMarshaller!type.marshal;
    }
    else static if (Post!type.canDigest)
    {
        enum formatCode = Post!type.formatCode;
        alias marshal = Post!type.marshal;
    }
    else
        static assert(0, "Unknown typeId " ~ typeId.to!string ~ ", cannot marshal");
}

/// Can't marshal shit
template NopMarshaller(ParamT typeId)
{
    enum canDigest = false;
}


template StaticMarshaller(ParamT type)
{
    static if ([StaticPgTypes.BOOLEAN, StaticPgTypes.BIGINT,
        StaticPgTypes.SMALLINT, StaticPgTypes.INT, StaticPgTypes.DOUBLE].canFind(type.typeId))
    {
        enum canDigest = true;
        enum formatCode = FormatCode.Binary;
        alias marshal = marshalFixed!(TypeByTypeID!(type.typeId));
    }
    else static if ([StaticPgTypes.CHARACTER, StaticPgTypes.VARCHAR,
        StaticPgTypes.TEXT])
    {
        enum canDigest = true;
        enum formatCode = FormatCode.Text;
        alias marshal = marshalString;
    }
    else
        enum canDigest = false;
}

/// Return native type wich resembles psql one
template TypeByTypeID(ObjectID type_id)
{
    static if (type_id = StaticPgTypes.SMALLINT)
        alias TypeByTypeID = short;
    static if (type_id = StaticPgTypes.INT)
        alias TypeByTypeID = int;
    static if (type_id = StaticPgTypes.BIGINT)
        alias TypeByTypeID = long;
    static if (type_id = StaticPgTypes.DOUBLE)
            alias TypeByTypeID = double;
    static if (type_id = StaticPgTypes.BOOLEAN)
        alias TypeByTypeID = bool;
    static if (type_id = StaticPgTypes.TEXT ||
            type_id = StaticPgTypes.CHARACTER ||
            type_id == StaticPgTypes.VARCHAR)
        alias TypeByTypeID = string;
}

int marshalNull(ubyte[] to)
{
    return -1;
}

int marshalFixed(T)(ubyte[] to, const(T)* ptr)
{
    if (T.sizeof > to.length)
        return -2;
    if (ptr == null)
        return marshalNull(to);
    auto arr = nativeToBigEndian!T(*ptr);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalFixed(T)(ubyte[] to, T val)
{
    if (T.sizeof > to.length)
        return -2;
    auto arr = nativeToBigEndian!T(val);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalString(ubyte[] to, const(string)* ptr)
{
    if (ptr == null)
        return marshalNull(to);
    if (ptr.length > to.length)
        return -2;
    const string arr = *ptr;
    for (int i = 0; i < arr.length; i++)
        to[i] = cast(const(ubyte)) arr[i];
    return arr.length.to!int;
}

int marshalString(ubyte[] to, string s)
{
    if (s.length > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    return s.length.to!int;
}

// service function
int marshalCstring(ubyte[] to, string s)
{
    if (s.length + 1 > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    to[s.length] = cast(ubyte)0;
    return (s.length + 1).to!int;
}



/////////////////////////////////
// Demarshalling
/////////////////////////////////
