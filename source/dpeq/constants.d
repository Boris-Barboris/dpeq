/**
Wire protocol constants.

Copyright: Boris-Barboris 2017-2019.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.constants;


/// dpeq implements only this version of wire protocol.
enum PROTOCOL_VERSION_MAJOR = 3;
enum PROTOCOL_VERSION_MINOR = 0;

/// Based on technical limitations of postgres.
enum ESTIMATE_MAX_FIELDS_IN_ROW = 2048;

/// Format of a serialized value.
enum FormatCode: short
{
    TEXT = 0,
    BINARY = 1,
}

/// Small portion of statically known oids of Postgres types.
enum KnownTypeOID: int
{
    NULL = 0,
    BOOLEAN = 16,
    BYTEA = 17,
    CHARACTER = 18,
    NAME = 19,
    BIGINT = 20,    /// int8
    SMALLINT = 21,  /// int2
    INT = 23,       /// int4
    TEXT = 25,
    OID = 26,
    TID = 27,
    XID = 28,
    CID = 29,
    PG_TYPE = 71,
    JSON = 114,
    XML = 142,
    POINT = 600,
    PATH = 602,
    BOX = 603,
    POLYGON = 604,
    LINE = 628,
    CIDR = 650,
    REAL = 700,     /// 32-bit float
    DOUBLE_PRECISION = 701,   /// 64-bit double, is actually called 'double precision'
    ABSTIME = 702,
    UNKNOWN = 705,
    CIRCLE = 718,
    MONEY = 790,
    INET = 869,
    VARCHAR = 1043,
    DATE = 1082,
    TIME = 1083,
    TIMESTAMP = 1114,
    TIMESTAMPTZ = 1184,
    INTERVAL = 1186,
    TIMETZ = 1266,
    BIT = 1560,
    VARBIT = 1562,
    NUMERIC = 1700,
    /**
    "Another special case is that a parameter's type can be specified as void
    (that is, the OID of the void pseudo-type). This is meant to allow
    parameter symbols to be used for function parameters that are actually OUT
    parameters. Ordinarily there is no context in which a void parameter could
    be used, but if such a parameter symbol appears in a function's parameter
    list, it is effectively ignored. For example, a function call such as
    foo($1,$2,$3,$4) could match a function with two IN and two OUT arguments,
    if $3 and $4 are specified as having type void."
    */
    VOID = 2278,
    UUID = 2950,
    JSONB = 3802
}

/// Messages, sent by frontend (postgres client).
/// https://www.postgresql.org/docs/current/protocol-message-formats.html
enum FrontendMessageType: char
{
    Bind = 'B',
    Close = 'C',
    CopyData = 'd',
    CopyDone = 'c',
    CopyFail = 'f',
    Describe = 'D',
    Execute = 'E',
    Flush = 'H',
    FunctionCall = 'F',
    Parse = 'P',
    PasswordMessage = 'p',
    Query = 'Q',
    Sync = 'S',
    Terminate = 'T'
}

/// Messages, sent by backend (postgres server).
/// https://www.postgresql.org/docs/current/protocol-message-formats.html
enum BackendMessageType: char
{
    Authentication = 'R',
    BackendKeyData = 'K',
    BindComplete = '2',         /// header-only message, no body
    CloseComplete = '3',        /// header-only message, no body
    CommandComplete = 'C',
    CopyData = 'd',
    CopyDone = 'c',             /// header-only message, no body
    CopyInResponse = 'G',
    CopyOutResponse = 'H',
    CopyBothResponse = 'W',
    DataRow = 'D',
    EmptyQueryResponse = 'I',   /// header-only message, no body
    ErrorResponse = 'E',
    FunctionCallResponse = 'V',
    NoData = 'n',               /// header-only message, no body
    NoticeResponse = 'N',
    NotificationResponse = 'A',
    ParameterDescription = 't',
    ParameterStatus = 'S',
    ParseComplete = '1',        /// header-only message, no body
    PortalSuspended = 's',      /// header-only message, no body
    ReadyForQuery = 'Z',
    RowDescription = 'T',
    NegotiateProtocolVersion = 'b'
}

/// Content of the ReadyForQuery response message, wich indicates
/// backend transaction status at the moment the message was sent.
enum TransactionStatus: char
{
    IDLE = 'I',     /// not in transaction block.
    INSIDE = 'T',   /// inside transaction block.
    /// in failed transaction block (queries will be rejected until block is ended).
    FAILED = 'E'
}

enum PreparedStatementOrPortal: char
{
    PREPARED_STATEMENT = 'S',
    PORTAL = 'P'
}

enum int AUTHENTICATION_SUCCESS = 0;

/// Known authentication protocol designator, first field in Authentication***
/// backend message contents.
enum AuthenticationProtocol: int
{
    KERBEROSV5 = 2,
    CLEARTEXT_PASSWORD = 3,
    MD5_PASSWORD = 5,
    SCMC_CREDENTIALS = 6,
    GSSAPI = 7,
    SSPI = 9,
    GCC_CONTINUE = 8,    /// message contains GSSAPI or SSPI data
    SASL = 10,
    SASL_CONTINUE = 11,
    SASL_FINAL = 12
}