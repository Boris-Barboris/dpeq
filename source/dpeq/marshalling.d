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


/** Default compile-time one-to-one mapper, wich for ObjectID of some type
* gives it's native type representation, and marshalling and demarshalling
* functions. You can extend it with two custom mappers: Pre and Post. */
template DefaultFieldMarshaller(FieldSpec field, alias Pre = NopMarshaller,
    alias Post = NopMarshaller)
{
    static if (Pre!field.canDigest)
    {
        // must alias to native type representing this value
        alias type = Pre!field.type;

        // By default, this format code will be passed in Bind message
        // as parameter formatCode.
        enum formatCode = Pre!type.formatCode;

        // demarshaller must accept all formatCodes that make sence for this type.
        alias demarshal = Pre!field.demarshal;

        // marshaller only needs to support one formatCode, mentioned above
        alias marshal = Pre!field.marshal;
    }
    else static if (StaticFieldMarshaller!field.canDigest)
    {
        alias type = StaticFieldMarshaller!field.type;
        enum formatCode = StaticFieldMarshaller!field.formatCode;
        alias demarshal = StaticFieldMarshaller!field.demarshal;
        alias marshal = StaticFieldMarshaller!field.marshal;
    }
    else static if (Post!field.canDigest)
    {
        alias type = Post!field.type;
        enum formatCode = Post!type.formatCode;
        alias demarshal = Post!field.demarshal;
        alias marshal = Post!field.marshal;
    }
    else
        static assert(0, "Unknown typeId " ~ field.typeId.to!string ~
            ", cannot demarshal");
}

/// Can't marshal shit
template NopMarshaller(FieldSpec type)
{
    enum canDigest = false;
}


/// Types handled by dpeq natively
template StaticFieldMarshaller(FieldSpec field)
{
    static if (field.typeId == StaticPgTypes.BOOLEAN)
        mixin MarshTemplate!(bool, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.BIGINT)
        mixin MarshTemplate!(long, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.SMALLINT)
        mixin MarshTemplate!(short, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.INT)
        mixin MarshTemplate!(int, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.CHARACTER)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.VARCHAR)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.TEXT)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else
        enum canDigest = false;
}

// to prevent code duplication
mixin template MarshTemplate(NativeT, FormatCode fcode, string suffix)
{
    enum canDigest = true;
    enum formatCode = fcode;
    static if (field.nullable)
    {
        alias type = Nullable!NativeT;
        mixin("alias demarshal = demarshalNullable" ~ suffix ~ "!(NativeT);");
        mixin("alias marshal = marshalNullable" ~ suffix ~ "!(NativeT);");
    }
    else
    {
        alias type = NativeT;
        mixin("alias demarshal = demarshal" ~ suffix ~ "!(NativeT);");
        mixin("alias marshal = marshal" ~ suffix ~ "!(NativeT);");
    }
}



///////////////////////////////////////////////////////////////////////////
// Marshaller implementations. Marshaller writes only body of the data
// and returns count of bytes written, -1 if it's a null value,
// and -2 if the buffer is too small to fit whole value.
///////////////////////////////////////////////////////////////////////////


pragma(inline)
int marshalNull(ubyte[] to)
{
    return -1;  // special case, -1 length is null value in eq protocol.
}

// I don't really know how versatile are these functions, so let's keep
// them FixedField instead of NumericField

int marshalNullableFixedField(T)(ubyte[] to, in Nullable!T ptr)
{
    if (T.sizeof > to.length)
        return -2;
    if (ptr.isNull)
        return marshalNull(to);
    auto arr = nativeToBigEndian!T(ptr.get);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalFixedField(T)(ubyte[] to, in T val)
{
    if (T.sizeof > to.length)
        return -2;
    auto arr = nativeToBigEndian!T(val);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalNullableStringField(Dummy = void)(ubyte[] to, in Nullable!string val)
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

int marshalStringField(Dummy = void)(ubyte[] to, in string s)
{
    if (s.length > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    return s.length.to!int;
}

/// service function, for protocol messages.
/// Data strings are passed without trailing nulls.
int marshalCstring(ubyte[] to, in string s)
{
    if (s.length + 1 > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    to[s.length] = cast(ubyte)0;
    return (s.length + 1).to!int;
}



//////////////////////////////////////////////////////////////////////////////
// Demarshalling implementations. Demarshallers take byte array that contains
// data body, it's format code and length according to field prefix.
//////////////////////////////////////////////////////////////////////////////


/// Simple demarshal of some integer type.
pragma(inline)
T demarshalNumber(T = int)(const(ubyte)[] from)
    if (isNumeric!T)
{
    return bigEndianToNative!T(from[0 .. T.sizeof]);
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

T demarshalFixedField(T)(const(ubyte)[] from, in FormatCode fCode, in int len)
{
    enforce!PsqlClientException(len > 0, "zero-sized fixed non-nullable field");
    if (fCode == FormatCode.Binary)
    {
        enforce!PsqlClientException(len == T.sizeof, "Field size mismatch");
        return bigEndianToNative!T(from[0 .. T.sizeof]);
    }
    else if (fCode == FormatCode.Text)
        return demarshalString(from[0 .. len], len).to!T;
    else
        throw new PsqlClientException("Unsupported FormatCode");
}

Nullable!T demarshalNullableFixedField(T)
    (const(ubyte)[] from, in FormatCode fCode, in int len)
{
    if (len == -1)
        return Nullable!T.init;
    if (fCode == FormatCode.Binary)
    {
        enforce!PsqlClientException(len == T.sizeof, "Field size mismatch, " ~
            T.stringof ~ ", actual = " ~ len.to!string);
        return Nullable!T(bigEndianToNative!T(from[0 .. T.sizeof]));
    }
    else if (fCode == FormatCode.Text)
        return Nullable!T(demarshalString(from[0 .. len], len).to!T);
    else
        throw new PsqlClientException("Unsupported FormatCode");
}

string demarshalStringField(Dummy = void)
    (const(ubyte)[] from, in FormatCode fc, in int len)
{
    assert(fc == FormatCode.Text, "binary string?");
    enforce!PsqlClientException(len >= 0, "null string in non-nullable demarshaller");
    if (len == 0)
        return "";
    string res = (cast(immutable(char)*)(from.ptr))[0 .. len.to!size_t];
    return res;
}

/// returns inplace-constructed string without allocations. Hacky.
Nullable!string demarshalNullableStringField(Dummy = void)
    (const(ubyte)[] from, in FormatCode fc, in int len)
{
    assert(fc == FormatCode.Text, "binary string?");
    if (len == -1)
        return Nullable!string.init;
    if (len == 0)
        return Nullable!string("");
    string res = (cast(immutable(char)*)(from.ptr))[0 .. len.to!size_t];
    return Nullable!string(res);
}

/// service function
string demarshalString(const(ubyte)[] from, in size_t length)
{
    return (cast(immutable(char)*)(from.ptr))[0 .. length];
}



///////////////////////////////////////////
// Dynamic conversion code
///////////////////////////////////////////


// prototype of variant demarshaller
alias VariantDemarshaller =
    Variant function(const(ubyte)[] buf, in FormatCode fc, in int len);

template NopConverter()
{
    void registerDemarshallers(ref VariantDemarshaller[ObjectID] dict) {}
}

Variant wrapToVariant(alias f)(const(ubyte)[] buf, in FormatCode fc, in int len)
{
    return Variant(f(buf, fc, len));
}


/// Default converter hash map. You can extend it, or define your own.
class VariantConverter(alias Pre, alias Post)
{
    static VariantDemarshaller[ObjectID] demarshallers;

    static this()
    {
        Pre.registerDemarshallers(demarshallers);
        registerDemarshallers(demarshallers);
        Post.registerDemarshallers(demarshallers);
    }

    static void registerDemarshallers(ref VariantDemarshaller[ObjectID] dict)
    {
        // iterate over StaticPgTypes and take demarshallers from StaticFieldMarshaller
        foreach (em; __traits(allMembers, StaticPgTypes))
        {
            enum FieldSpec spec =
                FieldSpec(__traits(getMember, StaticPgTypes, em), true);
            static if (StaticFieldMarshaller!spec.canDigest)
            {
                /*pragma(msg, "registering default demarshaller for ",
                    StaticFieldMarshaller!spec.type, " in hash table");*/
                dict[spec.typeId] = &wrapToVariant!(StaticFieldMarshaller!spec.demarshal);
            }
        }
    }

    static Variant demarshal(const(ubyte)[] fieldBody, ObjectID type,
        FormatCode fc, int len)
    {
        return demarshallers[type](fieldBody, fc, len);
    }
}

alias NopedDefaultConverter = VariantConverter!(NopConverter!(), NopConverter!());
