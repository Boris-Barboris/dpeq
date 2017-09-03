/**
Constants of various nature.

Copyright: Copyright Boris-Barboris 2017.
License: MIT
Authors: Boris-Barboris
*/

module dpeq.constants;

/// transaction identifier
alias Xact = int;

/// Unique identifier of Psql object
alias ObjectID = int;

/// Format used in binding
enum FormatCode: short
{
    Text = 0,
    Binary = 1,
}

/** Small portion of statically known ids of Psql types, wich is enough
* to start connection and request full type list. And no, I'm not going
* to sit here and copypaste them, it's a job for run time. */
enum StaticPgTypes: ObjectID
{
    NULL = 0,
    BOOLEAN = 16,
    BYTEA = 17,
    CHARACTER = 18,
    NAME = 19,
    BIGINT = 20,
    SMALLINT = 21,
    INT = 23,
    TEXT = 25,
    OID = 26,
    TID = 27,
    XID = 28,
    CID = 29,
    PG_TYPE = 71,
    JSON = 114,
    XML = 142,
    REAL = 700,
    DOUBLE = 701,
    ABSTIME = 702,
    UNKNOWN = 705,
    MONEY = 790,
    VARCHAR = 1043,
    DATE = 1082,
    TIME = 1083,
    UUID = 2950,
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
