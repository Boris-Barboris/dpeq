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
import std.meta;
import std.traits;
import std.typecons: Nullable, Tuple;
import std.variant;
import std.uuid: UUID;

import dpeq.constants;
import dpeq.exceptions;



/// Value type descriptor, constisting of an oid of the type itself and a
/// boolean flag wich indicates wether the value can be null.
struct FieldSpec
{
    ObjectID typeId;
    bool nullable;
}


/** Default compile-time one-to-many mapper, wich for ObjectID of some Postgress type
* gives it's native type representation, and marshalling and demarshalling
* functions. You can extend it with two custom mappers: Pre and Post. */
template DefaultFieldMarshaller(FieldSpec field, alias Pre = NopMarshaller,
    alias Post = PromiscuousStringMarshaller)
{
    static if (Pre!field.canDigest)
    {
        // must alias to native type representing this value
        alias type = Pre!field.type;

        // By default, this format code will be passed in Bind message
        // as parameter formatCode.
        enum formatCode = Pre!field.formatCode;

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
        enum formatCode = Post!field.formatCode;
        alias demarshal = Post!field.demarshal;
        alias marshal = Post!field.marshal;
    }
    else
        static assert(0, "Unknown typeId " ~ field.typeId.to!string ~
            ", cannot (de)marshal");
}

/// Can't marshal shit
template NopMarshaller(FieldSpec type)
{
    enum canDigest = false;
}

/// This is a fallback marshaller that simply accepts any type as a string
template PromiscuousStringMarshaller(FieldSpec field)
{
    mixin MarshTemplate!(string, FormatCode.Text, "StringField");
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
    else static if (field.typeId == StaticPgTypes.OID)
        mixin MarshTemplate!(int, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.VARCHAR)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.CHARACTER)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.TEXT)
        mixin MarshTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.UUID)
        mixin MarshTemplate!(UUID, FormatCode.Binary, "UuidField");
    else static if (field.typeId == StaticPgTypes.REAL)
        mixin MarshTemplate!(float, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.DOUBLE)
        mixin MarshTemplate!(double, FormatCode.Binary, "FixedField");
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


template FCodeOfFSpec(alias Marsh = DefaultFieldMarshaller)
{
    template F(FieldSpec spec)
    {
        enum F = Marsh!spec.formatCode;
    }
}

/// Utility template to quickly get an array of format codes from an array of
/// FieldSpecs
template FSpecsToFCodes(FieldSpec[] specs, alias Marsh = DefaultFieldMarshaller)
{
    enum FSpecsToFCodes = [staticMap!(FCodeOfFSpec!Marsh.F, aliasSeqOf!specs)];
}

/*
///////////////////////////////////////////////////////////////////////////
// Marshaller implementations. Marshaller writes only body of the data
// and returns count of bytes written, -1 if it's a null value,
// and -2 if the buffer is too small to fit whole value.
///////////////////////////////////////////////////////////////////////////
*/

pragma(inline)
int marshalNull(ubyte[] to)
{
    return -1;  // special case, -1 length is null value in eq protocol.
}

// I don't really know how versatile are these functions, so let's keep
// them FixedField instead of NumericField

int marshalNullableFixedField(T)(ubyte[] to, lazy const(Nullable!T) ptr)
{
    if (ptr.isNull)
        return marshalNull(to);
    return marshalFixedField!T(to, ptr.get);
}

int marshalFixedField(T)(ubyte[] to, lazy const(T) val)
{
    if (T.sizeof > to.length)
        return -2;
    auto arr = nativeToBigEndian!T(val);
    to[0 .. arr.length] = arr;
    return arr.length;
}

int marshalNullableStringField(Dummy = void)(ubyte[] to, lazy const(Nullable!string) val)
{
    if (val.isNull)
        return marshalNull(to);
    return marshalStringField(to, val.get);
}

int marshalStringField(Dummy = void)(ubyte[] to, lazy const(string) s)
{
    if (s.length > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    return s.length.to!int;
}

/// Service function, used for marshalling of protocol messages.
/// Data strings are passed without trailing nulls.
int marshalCstring(ubyte[] to, lazy const(string) s)
{
    if (s.length + 1 > to.length)
        return -2;
    for (int i = 0; i < s.length; i++)
        to[i] = cast(const(ubyte)) s[i];
    to[s.length] = cast(ubyte)0;
    return (s.length + 1).to!int;
}

int marshalNullableUuidField(Dummy = void)(ubyte[] to, in Nullable!UUID val)
{
    if (val.isNull)
        return marshalNull(to);
    return marshalUuidField(to, val.get);
}

int marshalUuidField(Dummy = void)(ubyte[] to, in UUID val)
{
    if (to.length < 16)
        return -2;
    for (int i = 0; i < 16; i++)
        to[i] = val.data[i];
    return 16;
}

/*
//////////////////////////////////////////////////////////////////////////////
// Demarshalling implementations. Demarshallers take byte array that contains
// data body, it's format code and length according to field prefix.
//////////////////////////////////////////////////////////////////////////////
*/


/// Simple demarshal of some numeric type.
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

Nullable!UUID demarshalNullableUuidField(Dummy = void)
(const(ubyte)[] from, in FormatCode fc, in int len)
{
    if (len == -1)
        return Nullable!UUID.init;
    return Nullable!UUID(demarshalUuidField(from, fc, len));
}

UUID demarshalUuidField(Dummy = void)
(const(ubyte)[] from, in FormatCode fc, in int len)
{
    enforce!PsqlClientException(len > 0, "null uuid in non-nullable demarshaller");
    if (fc == FormatCode.Binary)
    {
        enforce!PsqlClientException(len == 16, "uuid is not 16-byte");
        ubyte[16] data;
        for (int i = 0; i < 16; i++)
            data[i] = from[i];
        return UUID(data);
    }
    else if (fc == FormatCode.Text)
    {
        scope string val = (cast(immutable(char)*)(from.ptr))[0 .. len.to!size_t];
        return UUID(val);
    }
    else
        throw new PsqlClientException("Unsupported FormatCode");
}


/*
/////////////////////////////////////////////////////////////////
// Dynamic conversion code, suitable for dynamic typing
/////////////////////////////////////////////////////////////////
*/

/// prototype of a nullable variant demarshaller, used in converter
alias VariantDemarshaller =
    NullableVariant function(const(ubyte)[] buf, in FormatCode fc, in int len);

/// std.variant.Variant subtype that is better suited for holding SQL null.
/// Null NullableVariant is essentially a valueless Variant instance.
struct NullableVariant
{
    Variant variant;
    alias variant this;

    this(T)(T value)
    {
        variant = value;
    }

    /// this property will be true if psql return null in this column
    @safe bool isNull() const { return !variant.hasValue; }

    string toString()
    {
        if (isNull)
            // may conflict with "null" string, but this can be said about any string
            return "null";
        else
            return variant.toString();
    }
}

NullableVariant wrapToVariant(alias f)(const(ubyte)[] buf, in FormatCode fc, in int len)
{
    auto nullableResult = f(buf, fc, len);
    if (nullableResult.isNull)
        return NullableVariant();
    else
        return NullableVariant(nullableResult.get);
}

/// Default converter hash map. You can extend it, or define your own.
class VariantConverter(alias Marsh = DefaultFieldMarshaller)
{
    static immutable VariantDemarshaller[ObjectID] demarshallers;

    @disable private this();

    shared static this()
    {
        VariantDemarshaller[ObjectID] aa;
        // iterate over StaticPgTypes and take demarshallers from StaticFieldMarshaller
        foreach (em; __traits(allMembers, StaticPgTypes))
        {
            // this assumes nullable return fields. Variant will wrap Nullable
            // of some native type.
            enum FieldSpec spec =
                FieldSpec(__traits(getMember, StaticPgTypes, em), true);
            /*pragma(msg, "registering default demarshaller for ",
                StaticFieldMarshaller!spec.type, " in hash table");*/
            aa[spec.typeId] = &wrapToVariant!(Marsh!spec.demarshal);
        }
        demarshallers = cast(immutable VariantDemarshaller[ObjectID]) aa;
    }

    static NullableVariant demarshal(
        const(ubyte)[] fieldBody, ObjectID type, FormatCode fc, int len)
    {
        immutable(VariantDemarshaller)* func = type in demarshallers;
        if (func)
            return (*func)(fieldBody, fc, len);
        else
        {
            // fallback to nullable string demarshaller
            if (fc == FormatCode.Text)
                return demarshallers[StaticPgTypes.VARCHAR](fieldBody, fc, len);
            else
                throw new PsqlClientException(
                    "Unable to deduce demarshaller for binary format of a type " ~ 
                    type.to!string);
        }
    }
}
