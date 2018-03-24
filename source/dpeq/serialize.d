/**
Primitives used for type (de)serialization.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.serialize;

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
    OID typeId;
    bool nullable;
}


/** Default compile-time one-to-many mapper, wich for OID of some Postgres type
* gives it's native type representation, and serializing and deserializing
* functions. You can extend it with two custom mappers: Pre and Post. */
template DefaultSerializer(FieldSpec field, alias Pre = NopSerializer,
    alias Post = StringSerializer)
{
    static if (Pre!field.canDigest)
    {
        // must be an alias to D type representing this value
        alias type = Pre!field.type;

        // default format code for this type
        enum formatCode = Pre!field.formatCode;

        // deserializer should accept all formatCodes that make sense for this
        // type, and it must throw in unhandled case.
        alias deserialize = Pre!field.deserialize;

        // serializer only needs to support one formatCode, mentioned above
        alias serialize = Pre!field.serialize;
    }
    else static if (StaticFieldSerializer!field.canDigest)
    {
        alias type = StaticFieldSerializer!field.type;
        enum formatCode = StaticFieldSerializer!field.formatCode;
        alias deserialize = StaticFieldSerializer!field.deserialize;
        alias serialize = StaticFieldSerializer!field.serialize;
    }
    else static if (Post!field.canDigest)
    {
        alias type = Post!field.type;
        enum formatCode = Post!field.formatCode;
        alias deserialize = Post!field.deserialize;
        alias serialize = Post!field.serialize;
    }
    else
        static assert(0, "Unknown typeId " ~ field.typeId.to!string ~
            ", cannot (de)serialize");
}

/// Can't serialize shit
template NopSerializer(FieldSpec type)
{
    enum canDigest = false;
}

/// This is a fallback serializeler that simply accepts any type as a string
template StringSerializer(FieldSpec field)
{
    mixin SerialTemplate!(string, FormatCode.Text, "StringField");
}

/// Types wich are well-known to dpeq
template StaticFieldSerializer(FieldSpec field)
{
    static if (field.typeId == StaticPgTypes.BOOLEAN)
        mixin SerialTemplate!(bool, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.BIGINT)
        mixin SerialTemplate!(long, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.SMALLINT)
        mixin SerialTemplate!(short, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.INT)
        mixin SerialTemplate!(int, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.OID)
        mixin SerialTemplate!(int, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.VARCHAR)
        mixin SerialTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.CHARACTER)
        mixin SerialTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.TEXT)
        mixin SerialTemplate!(string, FormatCode.Text, "StringField");
    else static if (field.typeId == StaticPgTypes.UUID)
        mixin SerialTemplate!(UUID, FormatCode.Binary, "UuidField");
    else static if (field.typeId == StaticPgTypes.REAL)
        mixin SerialTemplate!(float, FormatCode.Binary, "FixedField");
    else static if (field.typeId == StaticPgTypes.DOUBLE)
        mixin SerialTemplate!(double, FormatCode.Binary, "FixedField");
    else
        enum canDigest = false;
}

mixin template SerialTemplate(NativeT, FormatCode fcode, string suffix)
{
    enum canDigest = true;
    enum formatCode = fcode;
    static if (field.nullable)
    {
        alias type = Nullable!NativeT;
        mixin("alias deserialize = deserializeNullable" ~ suffix ~ "!(NativeT);");
        mixin("alias serialize = serializeNullable" ~ suffix ~ "!(NativeT);");
    }
    else
    {
        alias type = NativeT;
        mixin("alias deserialize = deserialize" ~ suffix ~ "!(NativeT);");
        mixin("alias serialize = serialize" ~ suffix ~ "!(NativeT);");
    }
}


template FCodeOfFSpec(alias Serializer = DefaultSerializer)
{
    template F(FieldSpec spec)
    {
        enum F = Serializer!spec.formatCode;
    }
}

/// Utility template to quickly get an array of format codes from an array of
/// FieldSpecs
template FSpecsToFCodes(FieldSpec[] specs, alias Serializer = DefaultSerializer)
{
    enum FSpecsToFCodes = [staticMap!(FCodeOfFSpec!Serializer.F, aliasSeqOf!specs)];
}

/*
///////////////////////////////////////////////////////////////////////////
// Serializer implementations. Serializer writes only body of the data
// and returns count of bytes written, -1 if it's a null value,
// and -2 if the buffer is too small to fit the whole value.
///////////////////////////////////////////////////////////////////////////
*/

@safe pure
{

    pragma(inline, true)
    int serializeNull(ubyte[] to) nothrow
    {
        return -1;  // special case, -1 length is null value in eq protocol.
    }

    // I don't really know how versatile are these functions, so let's keep
    // them FixedField instead of NumericField

    int serializeNullableFixedField(T)(ubyte[] to, in Nullable!T ptr) nothrow
    {
        if (ptr.isNull)
            return serializeNull(to);
        return serializeFixedField!T(to, ptr.get);
    }

    int serializeFixedField(T)(ubyte[] to, in T val) nothrow
    {
        if (T.sizeof > to.length)
            return -2;
        auto arr = nativeToBigEndian!T(val);
        to[0 .. arr.length] = arr;
        return arr.length;
    }

    int serializeNullableStringField(Dummy = void)(ubyte[] to, in Nullable!string val)
    {
        if (val.isNull)
            return serializeNull(to);
        return serializeStringField(to, val.get);
    }

    int serializeStringField(Dummy = void)(ubyte[] to, in string s)
    {
        if (s.length > to.length)
            return -2;
        for (int i = 0; i < s.length; i++)
            to[i] = cast(const(ubyte)) s[i];
        return s.length.to!int;
    }

    /// Service function, used for serializeling of protocol messages.
    /// Data strings are passed without trailing nulls.
    int serializeCstring(ubyte[] to, in string s)
    {
        if (s.length + 1 > to.length)
            return -2;
        for (int i = 0; i < s.length; i++)
            to[i] = cast(const(ubyte)) s[i];
        to[s.length] = cast(ubyte)0;
        return (s.length + 1).to!int;
    }

    int serializeNullableUuidField(Dummy = void)(ubyte[] to, in Nullable!UUID val) nothrow
    {
        if (val.isNull)
            return serializeNull(to);
        return serializeUuidField(to, val.get);
    }

    int serializeUuidField(Dummy = void)(ubyte[] to, in UUID val) nothrow
    {
        if (to.length < 16)
            return -2;
        for (int i = 0; i < 16; i++)
            to[i] = val.data[i];
        return 16;
    }

}

/*
//////////////////////////////////////////////////////////////////////////////
// Deserialing implementations. Deserializers take byte array that contains
// field data, it's format code and length, and return the resulting value or throw.
//////////////////////////////////////////////////////////////////////////////
*/

@safe pure
{

    /// Simple deserialize of some numeric type.
    pragma(inline)
    T deserializeNumber(T = int)(immutable(ubyte)[] from) nothrow
        if (isNumeric!T)
    {
        return bigEndianToNative!T(from[0 .. T.sizeof]);
    }

    /// psql uses `t` and `f` for boolean
    private bool to(T: bool)(in string s)
    {
        if (s == "t")
            return true;
        if (s == "f")
            return false;
        throw new PsqlSerializationException("Unable to deserialize bool from string " ~ s);
    }

    T deserializeFixedField(T)(immutable(ubyte)[] from, in FormatCode fCode, in int len)
    {
        enforce!PsqlSerializationException(len != -1, "null in not-null deserializer");
        enforce!PsqlSerializationException(len > 0, "zero-sized fixed field");
        if (fCode == FormatCode.Binary)
        {
            enforce!PsqlSerializationException(len == T.sizeof, "Field size mismatch");
            return bigEndianToNative!T(from[0 .. T.sizeof]);
        }
        else if (fCode == FormatCode.Text)
            return deserializeString(from[0 .. len]).to!T;
        else
            throw new PsqlSerializationException("Unsupported FormatCode");
    }

    Nullable!T deserializeNullableFixedField(T)
        (immutable(ubyte)[] from, in FormatCode fCode, in int len)
    {
        if (len == -1)
            return Nullable!T();
        return Nullable!T(deserializeFixedField!T(from, fCode, len));
    }

    string deserializeStringField(Dummy = void)
        (immutable(ubyte)[] from, in FormatCode fc, in int len)
    {
        enforce!PsqlSerializationException(len != -1, "null in not-null deserializer");
        enforce!PsqlSerializationException(fc == FormatCode.Text, "binary string");
        if (len == 0)
            return "";
        return cast(string) from[0 .. len.to!size_t];
    }

    /// returns inplace-constructed string without allocations. Hacky.
    Nullable!string deserializeNullableStringField(Dummy = void)
        (immutable(ubyte)[] from, in FormatCode fc, in int len)
    {
        if (len == -1)
            return Nullable!string();
        return Nullable!string(deserializeStringField(from, fc, len));
    }

    /// dpeq utility function
    string deserializeString(immutable(ubyte)[] from) nothrow
    {
        return cast(string) from[0 .. from.length];
    }

    /// dpeq utility function. Deserialize zero-terminated string from byte buffer.
    string deserializeProtocolString(immutable(ubyte)[] from, out size_t length)
    {
        size_t l = 0;
        while (from[l])
        {
            l++;
            if (l >= from.length)
                throw new PsqlSerializationException("Null-terminated string is not " ~
                    "null-terminated");
        }
        length = l + 1;
        if (l == 0)
            return string.init;
        return deserializeString(from[0..l]);
    }

    Nullable!UUID deserializeNullableUuidField(Dummy = void)
        (immutable(ubyte)[] from, in FormatCode fc, in int len)
    {
        if (len == -1)
            return Nullable!UUID();
        return Nullable!UUID(deserializeUuidField(from, fc, len));
    }

    UUID deserializeUuidField(Dummy = void)
        (immutable(ubyte)[] from, in FormatCode fc, in int len)
    {
        enforce!PsqlSerializationException(len != -1,
            "null uuid in non-null deserializer");
        if (fc == FormatCode.Binary)
        {
            enforce!PsqlSerializationException(len == 16, "uuid is not 16-byte");
            ubyte[16] data = from[0..16];
            return UUID(data);
        }
        else if (fc == FormatCode.Text)
        {
            string val = cast(string) from[0 .. len.to!size_t];
            return UUID(val);
        }
        else
            throw new PsqlSerializationException("Unsupported FormatCode");
    }

}


/*
/////////////////////////////////////////////////////////////////
// Dynamic conversion code, suitable for dynamic typing
/////////////////////////////////////////////////////////////////
*/

/// prototype of a nullable variant deserializer, used in converter
alias VariantDeserializer =
    NullableVariant function(immutable(ubyte)[] buf, in FormatCode fc, in int len) @system;

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
    bool isNull() const pure nothrow @safe { return !variant.hasValue; }

    string toString() @system
    {
        if (isNull)
            return "null";
        else
            return variant.toString();
    }

    NullableVariant opAssign(typeof(null) n) @system
    {
        variant = Variant();
        return this;
    }
}

unittest
{
    NullableVariant nv = NullableVariant("asd");
    assert(!nv.isNull);
    nv = null;
    assert(nv.isNull);
}

NullableVariant wrapToVariant(alias f)
    (immutable(ubyte)[] buf, in FormatCode fc, in int len) @system
{
    auto nullableResult = f(buf, fc, len);
    if (nullableResult.isNull)
        return NullableVariant();
    else
        return NullableVariant(nullableResult.get);
}

/// Default converter hash map. You can extend it, or define your own.
abstract class VariantConverter(alias Serzer = DefaultFieldSerializer)
{
    static immutable VariantDeserializer[OID] deserializers;

    @disable private this();

    shared static this()
    {
        VariantDeserializer[OID] aa;
        // iterate over StaticPgTypes and take deserializers from StaticFieldSerializer
        foreach (em; __traits(allMembers, StaticPgTypes))
        {
            // this assumes nullable return fields. Variant will wrap Nullable
            // of some native type.
            enum FieldSpec spec =
                FieldSpec(__traits(getMember, StaticPgTypes, em), true);
            aa[spec.typeId] = &wrapToVariant!(Serzer!spec.deserialize);
        }
        deserializers = cast(immutable VariantDeserializer[OID]) aa;
    }

    static NullableVariant deserialize(
        immutable(ubyte)[] fieldBody, OID type, FormatCode fc, int len) @system
    {
        immutable(VariantDeserializer)* func = type in deserializers;
        if (func)
            return (*func)(fieldBody, fc, len);
        else
        {
            // fallback to nullable string deserializer
            if (fc == FormatCode.Text)
                return deserializers[StaticPgTypes.VARCHAR](fieldBody, fc, len);
            else
                throw new PsqlSerializationException(
                    "Unable to deduce deserializer for binary format of a type " ~
                    type.to!string);
        }
    }
}
