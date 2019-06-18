/**
Structures that describe messages protocol messages.
Functions that (de)serialize end send/receive them.

Copyright: Boris-Barboris 2017-2019.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.messages;

import std.exception: enforce;
import std.conv: to;

import dpeq.exceptions;
import dpeq.connection;
import dpeq.constants;
import dpeq.serialization;
import dpeq.transport;


/// Message, received from backend.
struct RawBackendMessage
{
    BackendMessageType type;

    /// Raw message body excluding message type byte and
    /// first 4 bytes that represent length.
    ubyte[] data;
}

/// Message that can be sent to backend.
struct RawFrontendMessage
{
    @property FrontendMessageType type() const
    {
        assert(data.length > 0);
        return cast(FrontendMessageType) data[0];
    }

    /// Raw message body including header: message type byte and
    /// first 4 bytes that represent length.
    ubyte[] data;
}


RawBackendMessage receiveBackendMessage(IOpenTransport transport)
{
    RawBackendMessage res;
    ubyte[5] typeAndLength;
    transport.receive(typeAndLength[]);
    res.type = cast(BackendMessageType) typeAndLength[0];
    int msgLength = typeAndLength[1 .. 5].asPrimitive!int();
    enforce!PSQLProtocolException(msgLength >= 4, "invalid message length");
    if (msgLength > 4)
    {
        res.data = new ubyte[msgLength - 4];
        transport.receive(res.data);
    }
    return res;
}

/// Partially-deserialized message from Authentication* family.
struct AuthenticationMessage
{
    int protocol;
    ubyte[] data;
}

AuthenticationMessage receiveAuthenticationMessage(IOpenTransport transport)
{
    RawBackendMessage rawMessage = receiveBackendMessage(transport);
    enforce!PSQLProtocolException(
        rawMessage.type == BackendMessageType.Authentication,
        "expected Authentication message, received " ~ rawMessage.type);
    AuthenticationMessage res;
    res.protocol = rawMessage.data.consumePrimitive!int();
    res.data = rawMessage.data;
    return res;
}


/// Deserialized field (column) description, part of RowDescription message.
struct FieldDescription
{
    /// Name of the field. Spans original message buffer.
    string name;

    /// If the field can be identified as a column of a specific table,
    /// the object ID of the table; otherwise zero.
    int table;

    /// If the field can be identified as a column of a specific table,
    /// the attribute number of the column; otherwise zero.
    short columnId;

    /// The object ID of the field's data type.
    int type;

    /// The data type size (see pg_type.typlen).
    /// Note that negative values denote variable-width types.
    short typeLen;

    /// The type modifier (see pg_attribute.atttypmod).
    /// The meaning of the modifier is type-specific.
    int typeModifier;

    /// The format code being used for the field. Currently will be zero (text)
    /// or one (binary). In a RowDescription returned from the statement variant
    /// of Describe, the format code is not yet known and will always be zero.
    FormatCode formatCode;
}

/// Consume next FieldDescription message from 'data' buffer.
private FieldDescription consumeFieldDescription(ref ubyte[] data)
{
    FieldDescription result;
    result.name = data.consumeCString();
    result.table = data.consumePrimitive!int();
    result.columnId = data.consumePrimitive!short();
    result.type = data.consumePrimitive!int();
    result.typeLen = data.consumePrimitive!short();
    result.typeModifier = data.consumePrimitive!int();
    result.formatCode = data.consumePrimitive!FormatCode();
    return result;
}

/// Deserialized RowDescription message.
struct RowDescription
{
    /// All field descriptions.
    FieldDescription[] fieldDescriptions;

    /// parse from RawBackendMessage.data
    static RowDescription parse(ubyte[] data)
    {
        RowDescription res;
        short fieldCount = data.consumePrimitive!short();
        enforce!PSQLProtocolException(
            fieldCount >= 0 && fieldCount <= ESTIMATE_MAX_FIELDS_IN_ROW,
            "invalid fieldCount " ~ fieldCount.to!string);
        if (fieldCount > 0)
        {
            res.fieldDescriptions.length = fieldCount;
            for (int i = 0; i < fieldCount; i++)
                res.fieldDescriptions[i] = consumeFieldDescription(data);
        }
        enforce!PSQLProtocolException(
            data.length == 0, "Unconsumed data left in the buffer");
        return res;
    }
}

struct DataColumn
{
    bool isNull;
    ubyte[] value;
}

struct DataRow
{
    DataColumn[] columns;

    /// parse from RawBackendMessage.data
    static DataRow parse(ubyte[] data)
    {
        DataRow res;
        short columnCount = data.consumePrimitive!short();
        enforce!PSQLProtocolException(
            columnCount >= 0 && columnCount <= ESTIMATE_MAX_FIELDS_IN_ROW,
            "invalid columnCount " ~ columnCount.to!string);
        if (columnCount > 0)
        {
            res.columns.length = columnCount;
            for (int i = 0; i < columnCount; i++)
            {
                int columnLength = consumePrimitive!int(data);
                enforce!PSQLProtocolException(columnLength >= -1,
                    "invalid columnLength " ~ columnLength.to!string);
                res.columns[i].isNull = columnLength == -1;
                if (columnLength > 0)
                {
                    res.columns[i].value = data[0 .. columnLength];
                    data = data[columnLength .. $];
                }
            }
        }
        enforce!PSQLProtocolException(
            data.length == 0, "Unconsumed data left in the buffer");
        return res;
    }
}

/// NotificationResponse message received from the backend. Part of
/// NOTIFY mechanism of postgres.
/// https://www.postgresql.org/docs/current/static/sql-notify.html
struct NotificationResponse
{
    /// The process ID of the notifying backend process.
    int procId;

    /// The name of the channel that the notify has been raised on.
    string channel;

    /// The "payload" string passed by notifying process.
    string payload;

    /// parse from RawBackendMessage.data
    static NotificationResponse parse(ubyte[] data)
    {
        NotificationResponse res;
        res.procId = data.consumePrimitive();
        res.channel = data.consumeCString();
        res.payload = data.consumeCString();
        enforce!PSQLProtocolException(data.length == 0, "leftover data");
        return res;
    }
}

struct ReadyForQuery
{
    TransactionStatus transactionStatus;

    /// Parse from RawBackendMessage.data.
    static ReadyForQuery parse(ubyte[] data)
    {
        enforce!PSQLProtocolException(data.length == 1, "invalid message");
        return ReadyForQuery(data[0].to!TransactionStatus);
    }
}

/// Contents of NoticeResponse or ErrorResponse messages.
/// https://www.postgresql.org/docs/current/static/protocol-error-fields.html
struct NoticeOrError
{
    /** Field contents are ERROR, FATAL, or PANIC (in an error message), or
    WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a localized
    translation of one of these. Always present. */
    string severity;
    /** Field contents are ERROR, FATAL, or PANIC (in an error
    message), or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message).
    This is identical to the 'severity' field except that the contents are never
    localized. This is present only in messages generated by PostgreSQL
    versions 9.6 and later. */
    string severityV;
    /// https://www.postgresql.org/docs/current/static/errcodes-appendix.html
    char[5] code;
    /// Primary human-readable error message. This should be accurate
    /// but terse (typically one line). Always present.
    string message;
    /// Optional secondary error message carrying more detail about the
    /// problem. Might run to multiple lines.
    string detail;
    /** Optional suggestion what to do about the problem. This is intended to
    differ from Detail in that it offers advice (potentially inappropriate)
    rather than hard facts. Might run to multiple lines. */
    string hint;
    /** Decimal ASCII integer, indicating an error cursor position as an index
    into the original query string. The first character has index 1, and
    positions are measured in characters not bytes. */
    string position;
    /** this is defined the same as the position field, but it is used when
    the cursor position refers to an internally generated command rather than
    the one submitted by the client. The q field will always appear when this
    field appears.*/
    string internalPos;
    /// Text of a failed internally-generated command. This could be, for
    /// example, a SQL query issued by a PL/pgSQL function.
    string internalQuery;
    /** Context in which the error occurred. Presently this includes a call
    stack traceback of active procedural language functions and
    internally-generated queries. The trace is one entry per line, most
    recent first. */
    string where;
    string schema;
    string table;
    string column;
    /// If the error was associated with a specific data type, the name of
    /// the data type.
    string dataType;
    /** If the error was associated with a specific constraint, the name of the
    constraint. Refer to fields listed above for the associated table or domain.
    (For this purpose, indexes are treated as constraints, even if they weren't
    created with constraint syntax.) */
    string constraint;
    /// File name of the source-code location where the error was reported.
    string file;
    /// Line number of the source-code location where the error was reported.
    string line;
    /// Name of the source-code routine reporting the error.
    string routine;


    /// Parse from RawBackendMessage.data.
    static NoticeOrError parse(ubyte[] data)
    {
        NoticeOrError n;

        while (data.length > 0 && data[0] != 0)
        {
            char fieldType = data.consumePrimitive!char();
            // https://www.postgresql.org/docs/current/static/protocol-error-fields.html
            switch (fieldType)
            {
                case 'S':
                    n.severity = data.consumeCString();
                    break;
                case 'V':
                    n.severityV = data.consumeCString();
                    break;
                case 'C':
                    string code = data.consumeCString();
                    enforce!PSQLProtocolException(
                        code.length == 5, "Expected 5 bytes of SQLSTATE code");
                    n.code[] = code;
                    break;
                case 'M':
                    n.message = data.consumeCString();
                    break;
                case 'D':
                    n.detail = data.consumeCString();
                    break;
                case 'H':
                    n.hint = data.consumeCString();
                    break;
                case 'P':
                    n.position = data.consumeCString();
                    break;
                case 'p':
                    n.internalPos = data.consumeCString();
                    break;
                case 'q':
                    n.internalQuery = data.consumeCString();
                    break;
                case 'W':
                    n.where = data.consumeCString();
                    break;
                case 's':
                    n.schema = data.consumeCString();
                    break;
                case 't':
                    n.table = data.consumeCString();
                    break;
                case 'c':
                    n.column = data.consumeCString();
                    break;
                case 'd':
                    n.dataType = data.consumeCString();
                    break;
                case 'n':
                    n.constraint = data.consumeCString();
                    break;
                case 'F':
                    n.file = data.consumeCString();
                    break;
                case 'L':
                    n.line = data.consumeCString();
                    break;
                case 'R':
                    n.routine = data.consumeCString();
                    break;
                default:
                    data.consumeCString();
            }
        }
        enforce!PSQLProtocolException(
            data.length == 1, "Invalid message structure");
        return n;
    }
}


RawFrontendMessage buildPasswordMessage(string password)
{
    RawFrontendMessage res;
    int length = 4 + password.length.to!int + 1;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.PasswordMessage, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(password, slice);
    assert(slice.length == 0);
    return res;
}

RawFrontendMessage buildMD5PasswordMessage(string user, string password, ubyte[4] salt)
{
    // thank you, ddb authors
    static char[32] MD5toHex(T...)(T data)
    {
        import std.ascii: LetterCase;
        import std.digest.md: md5Of, toHexString;
        return md5Of(data).toHexString!(LetterCase.lower);
    }

    RawFrontendMessage res;
    int length =
        4 +
        (3 + 32) + 1; // 3 for md5 and 32 is hash size
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.PasswordMessage, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume("md5" ~ MD5toHex(MD5toHex(password, user), salt), slice);
    assert(slice.length == 0);
    return res;
}

/// 'startupParams' must contain 'user' and may contain 'database'.
/// 'startupParams' MUST NOT contain password.
ubyte[] buildStartupMessage(
    string[string] startupParams,
    short majorVersion = PROTOCOL_VERSION_MAJOR,
    short minorVersion = PROTOCOL_VERSION_MINOR)
{
    assert("user" in startupParams, "user is mandatory parameter");
    assert("password" !in startupParams, "password must be part of PasswordMessage");
    // StartupMessage does not have type header
    int length = 4 + 4 + 1;    // 1 is for null terminator in the end
    foreach (k, v; startupParams)
    {
        length += k.length + 1;
        length += v.length + 1;
    }
    ubyte[] res;
    res.length = length;
    ubyte[] slice = res;
    serializePrimitiveConsume(length, slice);
    serializePrimitiveConsume(majorVersion, slice);
    serializePrimitiveConsume(minorVersion, slice);
    foreach (k, v; startupParams)
    {
        serializeCStringConsume(k, slice);
        serializeCStringConsume(v, slice);
    }
    serializePrimitiveConsume(cast(ubyte) 0, slice);
    assert(slice.length == 0);
    return res;
}

ubyte[] buildSSLRequestMessage() nothrow
{
    ubyte[] res;
    res.length = int.sizeof * 2;
    ubyte[] slice = res;
    serializePrimitiveConsume(8, slice);
    serializePrimitiveConsume(80877103, slice);
    assert(slice.length == 0);
    return res;
}

ubyte[] buildCancelRequestMessage(BackendKeyData keyData)
{
    ubyte[] res;
    res.length = 16;
    ubyte[] slice = res;
    serializePrimitiveConsume(16, slice);
    serializePrimitiveConsume(80877102, slice);
    serializePrimitiveConsume(keyData.processId, slice);
    serializePrimitiveConsume(keyData.cancellationKey, slice);
    assert(slice.length == 0);
    return res;
}

RawFrontendMessage buildTerminateMessage() nothrow
{
    RawFrontendMessage res;
    res.data.length = 5;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Terminate, slice);
    serializePrimitiveConsume(4, slice);
    assert(slice.length == 0);
    return res;
}

struct BackendKeyData
{
    int processId;
    int cancellationKey;

    /// Parse from RawBackendMessage.data.
    static BackendKeyData parse(ubyte[] data)
    {
        BackendKeyData res;
        enforce!PSQLProtocolException(data.length == 8);
        res.processId = asPrimitive(data[0 .. 4]);
        res.cancellationKey = asPrimitive(data[4 .. 8]);
        return res;
    }
}

struct ParameterStatus
{
    string name;
    string value;

    /// Parse from RawBackendMessage.data.
    static ParameterStatus parse(ubyte[] data)
    {
        ParameterStatus res;
        res.name = data.consumeCString();
        res.value = data.consumeCString();
        enforce!PSQLProtocolException(data.length == 0, "leftover data");
        return res;
    }
}

struct ParameterDescription
{
    int[] paramTypeOIDs;

    /// Parse from RawBackendMessage.data.
    static ParameterDescription parse(ubyte[] data)
    {
        ParameterDescription res;
        short count = data.consumePrimitive!short();
        enforce!PSQLProtocolException(count >= 0);
        res.paramTypeOIDs.length = count;
        for (int i = 0; i < count; i++)
            res.paramTypeOIDs[i] = data.consumePrimitive!int();
        enforce!PSQLProtocolException(data.length == 0, "leftover data");
        return res;
    }
}

struct CommandComplete
{
    string commandTag;

    /// Parse from RawBackendMessage.data.
    static CommandComplete parse(ubyte[] data)
    {
        return CommandComplete(consumeCString(data));
    }
}

/// Bidy of CopyInResponse or CopyOutResponse or CopyBothResponse
struct CopyResponse
{
    /**
    0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator characters, etc). 1 indicates the overall copy format is binary (similar to DataRow format).
    */
    byte overallFormat;

    /**
    The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be zero if the overall copy format is textual.
    */
    short[] formatCodes;


    /// Parse from RawBackendMessage.data.
    static CopyResponse parse(ubyte[] data)
    {
        CopyResponse res;
        res.overallFormat = data.consumePrimitive!byte();
        short count = data.consumePrimitive!short();
        enforce!PSQLProtocolException(count >= 0);
        res.formatCodes.length = count;
        for (int i = 0; i < count; i++)
            res.formatCodes[i] = data.consumePrimitive!short();
        enforce!PSQLProtocolException(data.length == 0, "leftover data");
        return res;
    }
}

struct CopyData
{
    /**
    Data that forms part of a COPY data stream. Messages sent from the backend will always correspond to single data rows, but messages sent by frontends might divide the data stream arbitrarily.
    */
    ubyte[] data;

    /// Parse from RawBackendMessage.data.
    static CopyData parse(ubyte[] data)
    {
        return CopyData(data);
    }
}

RawFrontendMessage buildCopyDataMessage(ubyte[] data)
{
    RawFrontendMessage res;
    int length = (4 + data.length).to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.CopyData, slice);
    serializePrimitiveConsume(length, slice);
    slice[] = data[];
    return res;
}

RawFrontendMessage buildCopyFailMessage(string errorMsg)
{
    RawFrontendMessage res;
    int length = 4 + errorMsg.length.to!int + 1;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.CopyFail, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(errorMsg, slice);
    assert(slice.length == 0);
    return res;
}

RawFrontendMessage buildCopyDoneMessage() nothrow
{
    RawFrontendMessage res;
    res.data.length = 5;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.CopyDone, slice);
    serializePrimitiveConsume(4, slice);
    assert(slice.length == 0);
    return res;
}

//
// PLEASE READ THIS TO UNDERSTAND EXTENDED QUERY PROTOCOL:
// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
//

/** Build Query message, part of Simple Query subprotocol.

The simple Query message is approximately equivalent to the series Parse, Bind, portal Describe, Execute, Close, Sync, using the unnamed prepared statement and portal objects and no parameters. One difference is that it will accept multiple SQL statements in the query string, automatically performing the bind/describe/execute sequence for each one in succession. Another difference is that it will not return ParseComplete, BindComplete, CloseComplete, or NoData messages.
*/
RawFrontendMessage buildQueryMessage(string queryString)
{
    RawFrontendMessage res;
    int length = (4 + queryString.length + 1).to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Query, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(queryString, slice);
    assert(slice.length == 0);
    return res;
}

/** The Close message closes an existing prepared statement or portal and releases resources. It is not an error to issue Close against a nonexistent statement or portal name. The response is normally CloseComplete, but could be ErrorResponse if some difficulty is encountered while releasing resources. Note that closing a prepared statement implicitly closes any open portals that were constructed from that statement.
*/
RawFrontendMessage buildCloseMessage(PreparedStatementOrPortal kind, string name)
{
    RawFrontendMessage res;
    int length = (4 + 1 + name.length + 1).to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Close, slice);
    serializePrimitiveConsume(length, slice);
    serializePrimitiveConsume(kind, slice);
    serializeCStringConsume(name, slice);
    assert(slice.length == 0);
    return res;
}

/**
The Flush message does not cause any specific output to be generated, but forces the backend to deliver any data pending in its output buffers. A Flush must be sent after any extended-query command except Sync, if the frontend wishes to examine the results of that command before issuing more commands. Without Flush, messages returned by the backend will be combined into the minimum possible number of packets to minimize network overhead.
*/
RawFrontendMessage buildFlushMessage()
{
    RawFrontendMessage res;
    res.data.length = 5;
    serializePrimitive(FrontendMessageType.Flush, res.data[0 .. 1]);
    serializePrimitive(4, res.data[1 .. 5]);
    return res;
}

/**
At completion of each series of extended-query messages, the frontend should issue a Sync message. This parameterless message causes the backend to close the current transaction if it's not inside a BEGIN/COMMIT transaction block (“close” meaning to commit if no error, or roll back if error). Then a ReadyForQuery response is issued. The purpose of Sync is to provide a resynchronization point for error recovery. When an error is detected while processing any extended-query message, the backend issues ErrorResponse, then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal message processing. (But note that no skipping occurs if an error is detected while processing Sync — this ensures that there is one and only one ReadyForQuery sent for each Sync.)
*/
RawFrontendMessage buildSyncMessage()
{
    RawFrontendMessage res;
    res.data.length = 5;
    serializePrimitive(FrontendMessageType.Sync, res.data[0 .. 1]);
    serializePrimitive(4, res.data[1 .. 5]);
    return res;
}

/**
The Describe message (portal variant) specifies the name of an existing portal (or an empty string for the unnamed portal). The response is a RowDescription message describing the rows that will be returned by executing the portal; or a NoData message if the portal does not contain a query that will return rows; or ErrorResponse if there is no such portal.

The Describe message (statement variant) specifies the name of an existing prepared statement (or an empty string for the unnamed prepared statement). The response is a ParameterDescription message describing the parameters needed by the statement, followed by a RowDescription message describing the rows that will be returned when the statement is eventually executed (or a NoData message if the statement will not return rows). ErrorResponse is issued if there is no such prepared statement. Note that since Bind has not yet been issued, the formats to be used for returned columns are not yet known to the backend; the format code fields in the RowDescription message will be zeroes in this case.
*/
RawFrontendMessage buildDescribeMessage(
    PreparedStatementOrPortal kind, string name)
{
    RawFrontendMessage res;
    int length = (4 + 1 + name.length + 1).to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Describe, slice);
    serializePrimitiveConsume(length, slice);
    serializePrimitiveConsume(kind, slice);
    serializeCStringConsume(name, slice);
    assert(slice.length == 0);
    return res;
}

/**
If successfully created, a named prepared-statement object lasts till the end of the current session, unless explicitly destroyed. An unnamed prepared statement lasts only until the next Parse statement specifying the unnamed statement as destination is issued. (Note that a simple Query message also destroys the unnamed statement.) Named prepared statements must be explicitly closed before they can be redefined by another Parse message, but this is not required for the unnamed statement. Named prepared statements can also be created and accessed at the SQL command level, using PREPARE and EXECUTE.

From official docs about 'paramTypes':
Note that this is not an indication of the number of parameters that might appear
in the query string, only the number that the frontend wants to prespecify types for.
Each element specifies the object ID of the parameter data type. Placing a zero here is
equivalent to leaving the type unspecified.

Additional notes from postgres site:
1).
A parameter data type can be left unspecified by setting it to zero, or by making the array of parameter type OIDs shorter than the number of parameter symbols ($n) used in the query string. Another special case is that a parameter's type can be specified as void (that is, the OID of the void pseudo-type). This is meant to allow parameter symbols to be used for function parameters that are actually OUT parameters. Ordinarily there is no context in which a void parameter could be used, but if such a parameter symbol appears in a function's parameter list, it is effectively ignored. For example, a function call such as foo($1,$2,$3,$4) could match a function with two IN and two OUT arguments, if $3 and $4 are specified as having type void.
2).
The query string contained in a Parse message cannot include more than one SQL statement; else a syntax error is reported. This restriction does not exist in the simple-query protocol, but it does exist in the extended protocol, because allowing prepared statements or portals to contain multiple commands would complicate the protocol unduly.
*/
RawFrontendMessage buildParseMessage(
    string statement, string query, int[] paramTypes)
{
    RawFrontendMessage res;
    int length = to!int(
        4 +
        statement.length + 1 +
        query.length + 1 +
        2 +
        paramTypes.length * 4);
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Parse, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(statement, slice);
    serializeCStringConsume(query, slice);
    serializePrimitiveConsume(paramTypes.length.to!short, slice);
    foreach (int pt; paramTypes)
        serializePrimitiveConsume(pt, slice);
    assert(slice.length == 0);
    return res;
}

struct BindParam
{
    const void* value;
    FormatCode formatCode;
    FieldSerializingFunction serializer;
}

/**
Once a prepared statement exists, it can be readied for execution using a Bind message. The Bind message gives the name of the source prepared statement (empty string denotes the unnamed prepared statement), the name of the destination portal (empty string denotes the unnamed portal), and the values to use for any parameter placeholders present in the prepared statement. The supplied parameter set must match those needed by the prepared statement. (If you declared any void parameters in the Parse message, pass NULL values for them in the Bind message.) Bind also specifies the format to use for any data returned by the query; the format can be specified overall, or per-column. The response is either BindComplete or ErrorResponse.
*/
RawFrontendMessage buildBindMessage(
    string portal,
    string statement,
    BindParam[] params,
    FormatCode[] resultFormatCodes)
{
    RawFrontendMessage res;
    size_t szLen =
        4 +
        portal.length + 1 +
        statement.length + 1 +
        2 + 2 * params.length +
        2 + 4 * params.length +
        2 + 2 * resultFormatCodes.length;
    // iterate over params and precalculate message length
    foreach (BindParam param; params)
    {
        enforce!PSQLSerializationException(
            param.serializer, "field serializer unspecified");
        int fieldLen = param.serializer(param.value, null, true);
        enforce!PSQLSerializationException(
            fieldLen >= -1, "Invalid field length reported by serializer");
        if (fieldLen == -1)     // null
            continue;
        szLen += fieldLen;
    }
    int length = szLen.to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Bind, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(portal, slice);
    serializeCStringConsume(statement, slice);
    // write format codes
    serializePrimitiveConsume(params.length.to!short, slice);
    foreach (BindParam param; params)
        serializePrimitiveConsume(param.formatCode, slice);
    // write values
    serializePrimitiveConsume(params.length.to!short, slice);
    foreach (BindParam param; params)
    {
        ubyte[] lengthSlice = slice[0 .. 4];
        slice = slice[4 .. $];
        int fieldLen = param.serializer(param.value, &slice, false);
        enforce!PSQLSerializationException(
            fieldLen >= -1, "Invalid field length reported by serializer");
        serializePrimitive(fieldLen, lengthSlice);
    }
    // write result format codes
    serializePrimitiveConsume(resultFormatCodes.length.to!short, slice);
    foreach (FormatCode fcode; resultFormatCodes)
        serializePrimitiveConsume(fcode, slice);
    assert(slice.length == 0);
    return res;
}


/**
Once a portal exists, it can be executed using an Execute message. The Execute message specifies the portal name (empty string denotes the unnamed portal) and a maximum result-row count (zero meaning “fetch all rows”). The result-row count is only meaningful for portals containing commands that return row sets; in other cases the command is always executed to completion, and the row count is ignored. The possible responses to Execute are the same as those described above for queries issued via simple query protocol, except that Execute doesn't cause ReadyForQuery or RowDescription to be issued.

If Execute terminates before completing the execution of a portal (due to reaching a nonzero result-row count), it will send a PortalSuspended message; the appearance of this message tells the frontend that another Execute should be issued against the same portal to complete the operation. The CommandComplete message indicating completion of the source SQL command is not sent until the portal's execution is completed. Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages: CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended.
*/
RawFrontendMessage buildExecuteMessage(string portal, int rowLimit = 0)
{
    RawFrontendMessage res;
    int length = (4 + portal.length + 1 + 4).to!int;
    res.data.length = length + 1;
    ubyte[] slice = res.data;
    serializePrimitiveConsume(FrontendMessageType.Execute, slice);
    serializePrimitiveConsume(length, slice);
    serializeCStringConsume(portal, slice);
    serializePrimitiveConsume(rowLimit, slice);
    assert(slice.length == 0);
    return res;
}



string toString(RawBackendMessage msg)
{
    try
    {
        BackendMessageType msgType = msg.type.to!BackendMessageType;
        switch (msgType)
        {
            case BackendMessageType.BackendKeyData:
                return BackendKeyData.parse(msg.data).to!string;
            case BackendMessageType.ParameterStatus:
                return ParameterStatus.parse(msg.data).to!string;
            case BackendMessageType.ReadyForQuery:
                return ReadyForQuery.parse(msg.data).to!string;
            case BackendMessageType.RowDescription:
                return RowDescription.parse(msg.data).to!string;
            case BackendMessageType.DataRow:
                return DataRow.parse(msg.data).to!string;
            case BackendMessageType.CommandComplete:
                return CommandComplete.parse(msg.data).to!string;
            case BackendMessageType.NoticeResponse:
            case BackendMessageType.ErrorResponse:
                return NoticeOrError.parse(msg.data).to!string;
            case BackendMessageType.NotificationResponse:
                return NotificationResponse.parse(msg.data).to!string;
            case BackendMessageType.CopyInResponse:
            case BackendMessageType.CopyOutResponse:
            case BackendMessageType.CopyBothResponse:
                return CopyResponse.parse(msg.data).to!string;
            default:
                return msgType.to!string ~ " " ~ msg.data.to!string;
        }
    }
    catch (Exception ex)
    {
        return msg.to!string;
    }
}