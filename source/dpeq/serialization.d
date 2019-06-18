/**
(De)serialization primitives.

Copyright: Boris-Barboris 2017-2019.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.serialization;

import std.exception: enforce;
import std.bitmanip: nativeToBigEndian, bigEndianToNative;
import std.conv: to;

public import std.uuid: UUID;
public import std.typecons: Nullable;

import dpeq.constants;
import dpeq.exceptions;


/**
Converts 'value', pointed to by val to PSQL wire protocol form with a
predetermined format code, writes it to the head of 'dest' and moves the
head of 'dest' forward till the first untouched byte.
If 'value' is nullptr or can be interpreted as NULL, MUST return -1 and
leave 'dest' untouched.
If 'calculateLength' is true, function MUST calculate and return the number
of bytes required to fully serialize the 'value', and leave 'dest' untouched.
Otherwise function performs serialization and returns number of
bytes actually written. Caller MUST guarantee that 'dest' is of sufficient length
by calling this function beforehand with 'calculateLength = true' parameter.
*/
alias FieldSerializingFunction =
    int function(const void* value, ubyte[] * dest, bool calculateLength);

/**
Converts serialized value in predetermined format,
represented by 'from' byte buffer to some native form in 'dest'.
If 'isNull' is true, MUST interpret 'from' buffer as empty and set 'dest' to the
value wich corresponds to NULL.
'dest' pointer MUST be valid.
Function SHOULD throw PSQLDeserializationException in case of errors.
*/
alias FieldDeserializingFunction =
    void function(bool isNull, ubyte[] from, void* dest);


//
// Serializers for simple known types
//

int serializePrimitiveFieldBinary(T)(
    const T* value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null)
        return -1;
    if (calculateLength)
        return cast(int)T.sizeof;
    assert(dest);
    (*dest)[0 .. T.sizeof] = nativeToBigEndian!T(*value);
    *dest = (*dest)[T.sizeof .. $];
    return T.sizeof;
}

int serializeNullablePrimitiveFieldBinary(T)(
    const Nullable!T * value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null || value.isNull)
        return -1;
    return serializePrimitiveFieldBinary!T(&value.get(), calculateLength, dest);
}

/// Only null 'value' pointer is interpreted as NULL. (*value is null) case
/// is assumed to empty array.
int serializeByteArrayField(
    const ubyte[] * value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null)
        return -1;
    if (value.length == 0)
        return 0;
    if (calculateLength)
        return value.length.to!int;
    assert(dest);
    enforce!PSQLSerializationException(
        value.length < cast(size_t)int.max - 1, "array too long to serialize");
    (*dest)[0 .. value.length] = (*value)[];
    *dest = (*dest)[value.length .. $];
    return cast(int)value.length;
}

int serializeNullableByteArrayField(
    const Nullable!(ubyte[]) * value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null || value.isNull)
        return -1;
    return serializeByteArrayField(&value.get(), dest, calculateLength);
}

int serializeUUIDFieldBinary(
    const UUID* value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null)
        return -1;
    if (calculateLength)
        return cast(int)UUID.sizeof;
    assert(dest);
    (*dest)[0 .. UUID.sizeof] = value.data[];
    *dest = (*dest)[UUID.sizeof .. $];
    return cast(int)UUID.sizeof;
}

int serializeNullableUUIDFieldBinary(
    const Nullable!UUID * value, ubyte[] * dest, bool calculateLength = false)
{
    if (value is null || value.isNull)
        return -1;
    return serializeUUIDFieldBinary(&value.get(), dest, calculateLength);
}

/// C-style null-terminated string serialization.
/// Not a FieldSerializingFunction.
void serializeCString(const(char)[] s, ubyte[] dest) nothrow
{
    dest[0 .. s.length] = cast(immutable(ubyte)[]) s;
    dest[s.length] = 0;
}

void serializeCStringConsume(const(char)[] s, ref ubyte[] dest) nothrow
{
    serializeCString(s, dest);
    dest = dest[s.length + 1 .. $];
}

/// Not a FieldSerializingFunction.
void serializePrimitive(T)(T value, ubyte[] dest) nothrow
{
    dest[0 .. T.sizeof] = nativeToBigEndian!T(value);
}

void serializePrimitiveConsume(T)(T value, ref ubyte[] dest) nothrow
{
    serializePrimitive(value, dest);
    dest = dest[T.sizeof .. $];
}


//
// Deserializers for simple known types
//

/// Simple deserialization of some numeric type. Not a FieldDeserializingFunction.
T asPrimitive(T = int)(ubyte[] from)
{
    enforce!PSQLDeserializationException(
        from.length >= T.sizeof, "unexpected end of buffer");
    return bigEndianToNative!T(from[0 .. T.sizeof]);
}

/// Simple deserialization of some numeric type. Not a FieldDeserializingFunction.
/// Moves 'front' of 'from' slice forward by T.sizeof bytes.
T consumePrimitive(T = int)(ref ubyte[] from)
{
    T res = asPrimitive!T(from);
    from = from[T.sizeof .. $];
    return res;
}

/// Deserialize C-String and consume it's length and zero terminator
/// from slice 'from'.
string consumeCString(ref ubyte[] from)
{
    size_t tookBytes;
    string res = deserializeCString(from, tookBytes);
    from = from[tookBytes .. $];
    return res;
}

/// psql uses `t` and `f` for boolean
private bool to(T: bool)(string s)
{
    if (s == "t")
        return true;
    if (s == "f")
        return false;
    throw new PSQLDeserializationException(
        "Unable to deserialize bool from string " ~ s);
}

void deserializePrimitiveField(T, FormatCode formatCode)(
    bool isNull, ubyte[] from, T* dest)
{
    assert(dest !is null);
    enforce!PSQLDeserializationException(!isNull,
        "null passed to non-nullable deserializer");
    static if (formatCode == FormatCode.BINARY)
    {
        enforce!PSQLDeserializationException(
            from.length == T.sizeof, "field length mismatch");
        *dest = bigEndianToNative!T(from[0 .. T.sizeof]);
    }
    else static if (formatCode == FormatCode.TEXT)
    {
        *dest = (cast(string)from).to!T;
    }
}

void deserializeNullablePrimitiveField(T, FormatCode formatCode)(
    bool isNull, ubyte[] from, Nullable!T * dest)
{
    assert(dest !is null);
    if (isNull)
    {
        *dest = Nullable!T();
        return;
    }
    T res;
    deserializePrimitiveField!(T, formatCode)(false, from, &res);
    *dest = Nullable!T(res);
}

/// Assigns 'dest' to 'from', re-using message memory buffer.
void deserializeByteArrayField(
    bool isNull, ubyte[] from, ubyte[] * dest)
{
    assert(dest !is null);
    enforce!PSQLDeserializationException(
        !isNull, "null passed to non-nullable deserializer");
    if (from.length == 0)
    {
        (*dest).length = 0;
        return;
    }
    *dest = cast(ubyte[]) from;
}

void deserializeNullableByteArrayField(
    bool isNull, ubyte[] from, Nullable!(ubyte[]) * dest)
{
    assert(dest !is null);
    if (isNull)
    {
        *dest = Nullable!(ubyte[])();
        return;
    }
    ubyte[] res;
    deserializeByteArrayField(false, from, &res);
    *dest = Nullable!(ubyte[])(res);
}

/// Deserialize zero-terminated string of unknown length from byte buffer.
string deserializeCString(ubyte[] from, out size_t tookBytes)
{
    enforce!PSQLDeserializationException(from.length > 0, "unexpected end of buffer");
    size_t l = 0;
    while (from[l])
    {
        l++;
        enforce!PSQLDeserializationException(
            l < from.length, "Protocol string is not null-terminated");
    }
    tookBytes = l + 1;
    if (l == 0)
        return string.init;
    return cast(string) from[0 .. l];
}

void deserializeUUIDField(FormatCode formatCode)(
    bool isNull, ubyte[] from, UUID* dest)
{
    assert(dest !is null);
    enforce!PSQLDeserializationException(!isNull, "null passed to non-nullable deserializer");
    static if (formatCode == FormatCode.BINARY)
    {
        enforce!PSQLDeserializationException(from.length == UUID.sizeof,
            "field length mismatch");
        ubyte[UUID.sizeof] staticArr = from[0 .. UUID.sizeof];
        *dest = UUID(staticArr);
    }
    else static if (formatCode == FormatCode.TEXT)
    {
        *dest = UUID(cast(string) from);
    }
}

void deserializeNullableUUIDField(FormatCode formatCode)(
    bool isNull, ubyte[] from, Nullable!UUID * dest)
{
    assert(val !is null);
    if (isNull)
    {
        *dest = Nullable!UUID();
        return;
    }
    UUID res;
    deserializeUuidField!formatCode(false, from, &res);
    *dest = Nullable!UUID(res);
}
