/**
Primitives used for marshalling.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.marshalling;

import std.algorithm: canFind;
import std.exception: enforce;
import std.bitmanip: nativeToBigEndian, bigEndianToNative;
import std.conv: to;
import std.traits;
import std.typecons: Nullable, Tuple;
import std.variant;

import dpeq.constants;
import dpeq.exceptions;


/// Query parameter type descriptor
struct FieldSpec
{
    ObjectID typeId;
    bool nullable;
}


/////////////////////////////////
// Marshalling
/////////////////////////////////


/** Default compile-time oriented marshaller.
* You can extend it with two custom marshallers: Pre and Post. */
template DefaultParamMarshaller(FieldSpec type, alias Pre = NopMarshaller,
    alias Post = NopMarshaller)
{
    static if (Pre!type.canDigest)
    {
        enum formatCode = Pre!type.formatCode;
        alias marshal = Pre!type.marshal;
    }
    else static if (StaticParamMarshaller!type.canDigest)
    {
        enum formatCode = StaticParamMarshaller!type.formatCode;
        alias marshal = StaticParamMarshaller!type.marshal;
    }
    else static if (Post!type.canDigest)
    {
        enum formatCode = Post!type.formatCode;
        alias marshal = Post!type.marshal;
    }
    else
        static assert(0, "Unknown typeId " ~ type.typeId.to!string ~
            ", cannot marshal");
}

/// Can't marshal shit
template NopMarshaller(FieldSpec type)
{
    enum canDigest = false;
}


template StaticParamMarshaller(FieldSpec type)
{
    static if ([StaticPgTypes.BOOLEAN, StaticPgTypes.BIGINT,
        StaticPgTypes.SMALLINT, StaticPgTypes.INT, StaticPgTypes.DOUBLE].canFind(type.typeId))
    {
        enum canDigest = true;
        enum formatCode = FormatCode.Binary;
        static if (type.nullable)
            alias marshal = marshalNullableFixed!(TypeByTypeID!(type.typeId));
        else
            alias marshal = marshalFixed!(TypeByTypeID!(type.typeId));
    }
    else static if ([StaticPgTypes.CHARACTER, StaticPgTypes.VARCHAR,
        StaticPgTypes.TEXT].canFind(type.typeId))
    {
        enum canDigest = true;
        enum formatCode = FormatCode.Text;
        static if (type.nullable)
            alias marshal = marshalNullableString;
        else
            alias marshal = marshalString;
    }
    else
        enum canDigest = false;
}


/// Return native type for field spec
template TypeByFieldSpec(FieldSpec pt)
{
    static if (pt.nullable)
        alias TypeByFieldSpec = Nullable!(TypeByTypeID!(cast(ObjectID) pt.typeId));
    else
        alias TypeByFieldSpec = TypeByTypeID!(cast(ObjectID) pt.typeId);
}


/// Return native type wich resembles psql one
template TypeByTypeID(ObjectID typeId)
{
    static if (typeId == StaticPgTypes.SMALLINT)
        alias TypeByTypeID = short;
    static if (typeId == StaticPgTypes.INT)
        alias TypeByTypeID = int;
    static if (typeId == StaticPgTypes.BIGINT)
        alias TypeByTypeID = long;
    static if (typeId == StaticPgTypes.DOUBLE)
            alias TypeByTypeID = double;
    static if (typeId == StaticPgTypes.BOOLEAN)
        alias TypeByTypeID = bool;
    static if (typeId == StaticPgTypes.TEXT ||
            typeId == StaticPgTypes.CHARACTER ||
            typeId == StaticPgTypes.VARCHAR)
        alias TypeByTypeID = string;
}


pragma(inline)
int marshalNull(ubyte[] to)
{
    return -1;
}

int marshalNullableFixed(T)(ubyte[] to, const Nullable!T ptr)
{
    if (T.sizeof > to.length)
        return -2;
    if (ptr.isNull)
        return marshalNull(to);
    auto arr = nativeToBigEndian!T(ptr.get);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalFixed(T)(ubyte[] to, const T val)
{
    if (T.sizeof > to.length)
        return -2;
    auto arr = nativeToBigEndian!T(val);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalNullableString(ubyte[] to, const Nullable!string val)
{
    if (val.isNull)
        return marshalNull(to);
    if (val.length > to.length)
        return -2;
    auto arr = val.get;
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

/// service function, for protocol messages.
/// Data strings are passed without trailing nulls.
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


/** Default compile-time oriented demarshaller.
* You can extend it with two custom demarshallers: Pre and Post. */
template DefaultFieldDemarshaller(FieldSpec type, alias Pre = NopMarshaller,
    alias Post = NopMarshaller)
{
    static if (Pre!type.canDigest)
    {
        alias demarshal = Pre!type.demarshal;
    }
    else static if (StaticFieldDemarshaller!type.canDigest)
    {
        alias demarshal = StaticFieldDemarshaller!type.demarshal;
    }
    else static if (Post!type.canDigest)
    {
        alias demarshal = Post!type.demarshal;
    }
    else
        static assert(0, "Unknown typeId " ~ type.typeId.to!string ~
            ", cannot demarshal");
}


/// Default well-known types
template StaticFieldDemarshaller(FieldSpec type)
{
    static if ([StaticPgTypes.BOOLEAN, StaticPgTypes.BIGINT,
        StaticPgTypes.SMALLINT, StaticPgTypes.INT, StaticPgTypes.DOUBLE].canFind(type.typeId))
    {
        enum canDigest = true;
        static if (type.nullable)
            alias demarshal = demarshalNullableFixedField!(TypeByTypeID!(type.typeId));
        else
            alias demarshal = demarshalFixedField!(TypeByTypeID!(type.typeId));
    }
    else static if ([StaticPgTypes.CHARACTER, StaticPgTypes.VARCHAR,
        StaticPgTypes.TEXT])
    {
        enum canDigest = true;
        static if (type.nullable)
            alias demarshal = demarshalNullableStringField;
        else
            alias demarshal = demarshalStringField;
    }
    else
        enum canDigest = false;
}



/// Simple demarshal of some integer type.
pragma(inline)
T demarshalNumber(T = int)(const(ubyte)[] from)
    if (isNumeric!T)
{
    return bigEndianToNative!T(from[0 .. T.sizeof]);
}

/// psql can return bigint (8 bytes) as one byte if it is small
T bigEndianToNativePsql(T)(const(ubyte)[] from)
{
    ubyte[T.sizeof] arr = 0;
    arr[T.sizeof - from.length .. $] = from;
    return bigEndianToNative!T(arr);
}

/// psql uses `t` and `f` for boolean
bool to(T: bool)(in string s)
{
    if (s == "t")
        return true;
    if (s == "f")
        return false;
    throw new PsqlClientException("Unable to unmarshal bool from string " ~ s);
}

T demarshalFixedField(T)(const(ubyte)[] from, FormatCode fCode, out int shift)
{
    int len = demarshalNumber(from[0 .. 4]);
    shift = 4 + len;
    if (fCode == FormatCode.Binary)
    {
        enforce!PsqlClientException(len <= T.sizeof, "Field size mismatch");
        return bigEndianToNativePsql!T(from[0 .. l]);
    }
    else if (fCode == FormatCode.Text)
        return demarshalString(from[4 .. 4 + len], len).to!T;
    else
        throw new PsqlClientException("Unsupported FormatCode");
}

Nullable!T demarshalNullableFixedField(T)(const(ubyte)[] from, FormatCode fCode, out int shift)
{
    int len = demarshalNumber(from[0 .. 4]);
    if (len == -1)
    {
        shift = 4;
        return Nullable!T.init;
    }
    shift = 4 + len;
    if (fCode == FormatCode.Binary)
    {
        enforce!PsqlClientException(len <= T.sizeof, "Field size mismatch, " ~
            T.stringof ~ ", actual = " ~ len.to!string);
        return Nullable!T(bigEndianToNativePsql!T(from[0 .. len]));
    }
    else if (fCode == FormatCode.Text)
        return Nullable!T(demarshalString(from[4 .. 4 + len], len).to!T);
    else
        throw new PsqlClientException("Unsupported FormatCode");
}

string demarshalStringField(const(ubyte)[] from, FormatCode fc, out int shift)
{
    assert(fc == FormatCode.Text, "binary string?");
    int len = demarshalNumber(from[0 .. 4]);
    enforce!PsqlClientException(len >= 0, "null string in non-nullable demarshaller");
    shift = 4 + len;
    if (len == 0)
        return "";
    string res = (cast(immutable(char)*)(from.ptr + 4))[0 .. len.to!size_t];
    return res;
}

/// returns inplace-constructed string without allocations. Hacky.
Nullable!string demarshalNullableStringField(const(ubyte)[] from, FormatCode fc, out int shift)
{
    assert(fc == FormatCode.Text, "binary string?");
    int len = demarshalNumber(from[0 .. 4]);
    if (len == -1)
    {
        shift = 4;
        return Nullable!string.init;
    }
    shift = 4 + len;
    if (len == 0)
        return Nullable!string("");
    string res = (cast(immutable(char)*)(from.ptr + 4))[0 .. len.to!size_t];
    return Nullable!string(res);
}

/// service function
string demarshalString(const(ubyte)[] from, size_t length)
{
    return (cast(immutable(char)*)(from.ptr))[0 .. length];
}



///////////////////////////////////////////
// Runtime-driven conversion code
///////////////////////////////////////////


alias RuntimeDemarshaller = Variant function(const(ubyte)[] buf, FormatCode fc, out int shift);


template NopConverter()
{
    void registerDemarshallers(ref RuntimeDemarshaller[ObjectID] dict) {}
}

Variant wrapToVariant(alias f)(const(ubyte)[] buf, FormatCode fc, out int shift)
{
    return Variant(f(buf, fc, shift));
}



/// Default converter hash map. You can extend it, or define your own.
class DefaultConverter(alias Pre, alias Post)
{
    static RuntimeDemarshaller[ObjectID] demarshallers;

    static this()
    {
        Pre.registerDemarshallers(demarshallers);
        registerDemarshallers(demarshallers);
        Post.registerDemarshallers(demarshallers);
    }

    static void registerDemarshallers(ref RuntimeDemarshaller[ObjectID] dict)
    {
        dict[StaticPgTypes.SMALLINT] = &wrapToVariant!(demarshalNullableFixedField!short);
        dict[StaticPgTypes.INT] = &wrapToVariant!(demarshalNullableFixedField!int);
        dict[StaticPgTypes.BIGINT] = &wrapToVariant!(demarshalNullableFixedField!long);
        dict[StaticPgTypes.BOOLEAN] = &wrapToVariant!(demarshalNullableFixedField!bool);
        dict[StaticPgTypes.TEXT] = &wrapToVariant!(demarshalNullableStringField);
        dict[StaticPgTypes.CHARACTER] = &wrapToVariant!(demarshalNullableStringField);
        dict[StaticPgTypes.VARCHAR] = &wrapToVariant!(demarshalNullableStringField);
    }

    static Variant demarshal(const(ubyte)[] fieldBody, ObjectID type,
        FormatCode fc, out int shift)
    {
        return demarshallers[type](fieldBody, fc, shift);
    }
}

alias NopedDefaultConverter = DefaultConverter!(NopConverter!(), NopConverter!());
