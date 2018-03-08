/**
Constants of various nature.

Copyright: Copyright Boris-Barboris 2017-2018.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.constants;


/// Unique identifier of a Psql object. Mostly used to identify a type in dpeq.
alias ObjectID = int;

/// Format of a marshalled value.
enum FormatCode: short
{
    Text = 0,
    Binary = 1,
}

/** Small portion of statically known ids of Psql types, wich is enough
* to start connection and request full type list. */
enum StaticPgTypes: ObjectID
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
    DOUBLE = 701,   /// 64-bit double, is actually called 'double precision'
    ABSTIME = 702,
    UNKNOWN = 705,
    CIRCLE = 718,
    MONEY = 790,
    INET = 869,
    VARCHAR = 1043,
    DATE = 1082,
    TIME = 1083,
    TIMESTAMP = 1114,
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

alias PgType = StaticPgTypes;

/// Returns postgress-compatible name of the type. Throws if type OID is
/// unknown.
string pgTypeName(ObjectID pgt)
{
    import std.conv: to;
    StaticPgTypes spgt = pgt.to!StaticPgTypes;
    switch (spgt)
    {
        case (StaticPgTypes.DOUBLE):
            return "double precision";
        default:
            return spgt.to!string;
    }
}

/// https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html
enum FrontMessageType: char
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

/// https://www.postgresql.org/docs/9.5/static/protocol-message-formats.html
enum BackendMessageType: char
{
    Authentication = 'R',
    BackendKeyData = 'K',
    BindComplete = '2',
    CloseComplete = '3',
    CommandComplete = 'C',
    CopyData = 'd',
    CopyDone = 'c',
    CopyInResponse = 'G',
    CopyOutResponse = 'H',
    CopyBothResponse = 'W',
    DataRow = 'D',
    EmptyQueryResponse = 'I',
    ErrorResponse = 'E',
    FunctionCallResponse = 'V',
    NoData = 'n',
    NoticeResponse = 'N',
    NotificationResponse = 'A',
    ParameterDescription = 't',
    ParameterStatus = 'S',
    ParseComplete = '1',
    PortalSuspended = 's',
    ReadyForQuery = 'Z',
    RowDescription = 'T'
}

/** Content of the ReadyForQuery response message, indicating backend
transaction status. */
enum TransactionStatus: char
{
    IDLE = 'I',     /// idle (not in transaction block)
    TBLOCK = 'T',   /// in transaction block
    /// in failed transaction block (queries will be rejected until block is ended)
    FAILEDBLOCK = 'E'
}