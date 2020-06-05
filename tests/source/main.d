/**
Simple tests that serve as an example.

Copyright: Copyright Boris-Barboris 2017-2020.
License: MIT
Authors: Boris-Barboris
*/

import core.thread;
import core.time;

import std.algorithm: map;
import std.array: array;
import std.ascii: isAlphaNum;
import std.conv: to;
import std.string: strip;
import std.exception: assumeWontThrow;
import std.typecons: Nullable, scoped;
import std.process: environment;
import std.stdio;
import std.uuid: UUID, randomUUID;

import dunit;

import dpeq;


final class DebugSocket: StdSocketTransport
{
    this(StdConnectParameters params)
    {
        super(params);
    }

    override void send(const(ubyte)[] buf)
    {
        writeln("sending ", buf.length, " bytes: ", buf);
        super.send(buf);
    }

    override void receive(ubyte[] dest)
    {
        writeln("receiving ", dest.length, " bytes...");
        super.receive(dest);
    }
}

final class DebugPSQLConnection: PSQLConnection
{
    this(ITransport transport, SSLPolicy sslPolicy)
    {
        super(transport, sslPolicy);
    }

    override RawBackendMessage receiveMessage()
    {
        RawBackendMessage msg = super.receiveMessage();
        writeln("received " ~ msg.toString);
        return msg;
    }
}


class PSQLConnectionTests
{
    mixin UnitTest;

    DebugPSQLConnection connection;
    DebugSocket transport;
    PasswordAuthenticator password;
    string[string] startupParams;
    bool isCockroach;

    @BeforeEach
    public void setUp()
    {
        isCockroach = environment.get("IS_COCKROACH", "false").to!bool;
        transport = new DebugSocket(
            StdConnectParameters(
                environment.get("TEST_DATABASE_HOST", "localhost"),
                environment.get("TEST_DATABASE_PORT", "5432").to!ushort
            ));
        transport.receiveTimeout = seconds(1);
        transport.sendTimeout = seconds(1);
        transport.connect();
        connection = new DebugPSQLConnection(transport, SSLPolicy.NEVER);
        startupParams.clear();
        startupParams["user"] = environment.get("TEST_USER", "postgres");
        startupParams["database"] = environment.get("TEST_DATABASE", "postgres");
        password = new PasswordAuthenticator(environment.get("TEST_PASSWORD", "postgres"));

        passwordAuth();
    }

    private void passwordAuth()
    {
        connection.handshakeAndAuthenticate(startupParams, password);
        assertTrue(!connection.closed);
        assertTrue(connection.authenticated);
        assertTrue(connection.isOpen);
        if (!isCockroach)
        {
            assertTrue(connection.backendKeyData.processId != 0);
            assertTrue(connection.backendKeyData.cancellationKey != 0);
        }
        assertEquals(TransactionStatus.IDLE, connection.lastTransactionStatus);
        writeln("Parameter statuses: ", connection.parameterStatuses);
    }

    @AfterEach
    public void tearDown()
    {
        if (connection)
            connection.close();
    }

    @Test
    void testSimpleQuerySelectVersion()
    {
        RawFrontendMessage query = buildQueryMessage("SELECT version()");
        connection.sendMessage(query);

        CommandComplete commandComplele;
        RowDescription rowDescription;
        DataRow dataRow;

        PollCallback poller = (PSQLConnection con, RawBackendMessage msg)
        {
            assert(con is connection);
            switch (msg.type)
            {
                case BackendMessageType.CommandComplete:
                    commandComplele = CommandComplete.parse(msg.data);
                    break;
                case BackendMessageType.RowDescription:
                    rowDescription = RowDescription.parse(msg.data);
                    break;
                case BackendMessageType.DataRow:
                    dataRow = DataRow.parse(msg.data);
                    break;
                default:
            }
            return PollAction.CONTINUE;
        };

        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(poller));
        assertEquals(TransactionStatus.IDLE, connection.lastTransactionStatus);
        assertEquals("SELECT 1", commandComplele.commandTag);
        assertEquals(KnownTypeOID.TEXT, rowDescription.fieldDescriptions[0].type);
        assertEquals(FormatCode.TEXT, rowDescription.fieldDescriptions[0].formatCode);
        assertTrue(!dataRow.columns[0].isNull);

        string returnedVersion;
        deserializeByteArrayField(
            dataRow.columns[0].isNull,
            dataRow.columns[0].value,
            cast(ubyte[]*) &returnedVersion);
        writeln("version() returned: ", returnedVersion);
    }

    @Test
    void testSimpleQueryErrorResponse()
    {
        RawFrontendMessage query = buildQueryMessage("SELECT nonexistingFunction()");
        connection.sendMessage(query);
        PollResult pr = connection.pollMessages((con, msg)
            {
                if (msg.type == BackendMessageType.ErrorResponse)
                {
                    NoticeOrError error = NoticeOrError.parse(msg.data);
                    assertEquals("ERROR", error.severity);
                    assertEquals("42883", error.code[]);
                    return PollAction.BREAK;
                }
                return PollAction.CONTINUE;
            });
        assertEquals(PollResult.POLL_CALLBACK_BREAK, pr);
    }

    @Test
    void testSimpleQueryEmptyQueryResponse()
    {
        RawFrontendMessage query = buildQueryMessage("");
        connection.sendMessage(query);
        bool receivedEmptyQuery;

        PollCallback poller = (PSQLConnection con, RawBackendMessage msg)
        {
            switch (msg.type)
            {
                case BackendMessageType.CommandComplete:
                    assert(0, "should not have been received");
                case BackendMessageType.EmptyQueryResponse:
                    receivedEmptyQuery = true;
                    break;
                default:
            }
            return PollAction.CONTINUE;
        };

        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(poller));
        assertEquals(TransactionStatus.IDLE, connection.lastTransactionStatus);
        assertTrue(receivedEmptyQuery);
    }

    @Test
    void testSimpleQueryTransaction()
    {
        RawFrontendMessage query = buildQueryMessage(
            "BEGIN;
            CREATE TABLE temptable (pk integer PRIMARY KEY);");
        connection.sendMessage(query);

        CommandComplete commandComplele;
        int commandsCompleted;

        PollCallback poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                assert(con is connection);
                switch (msg.type)
                {
                    case BackendMessageType.CommandComplete:
                        commandComplele = CommandComplete.parse(msg.data);
                        if (commandsCompleted == 0)
                            assertEquals("BEGIN", commandComplele.commandTag);
                        if (commandsCompleted == 1)
                            assertEquals("CREATE TABLE", commandComplele.commandTag);
                        if (++commandsCompleted > 2)
                            assert(0, "unexpected CommandComplete message");
                        break;
                    default:
                }
                return PollAction.CONTINUE;
            };

        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(poller));
        assertEquals(2, commandsCompleted);
        assertEquals(TransactionStatus.INSIDE, connection.lastTransactionStatus);

        connection.sendMessage(buildQueryMessage("ROLLBACK"));

        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                assert(con is connection);
                switch (msg.type)
                {
                    case BackendMessageType.CommandComplete:
                        commandComplele = CommandComplete.parse(msg.data);
                        if (commandsCompleted == 2)
                            assertEquals("ROLLBACK", commandComplele.commandTag);
                        if (++commandsCompleted > 3)
                            assert(0, "unexpected CommandComplete message");
                        break;
                    default:
                }
                return PollAction.CONTINUE;
            };

        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(poller));
        assertEquals(3, commandsCompleted);
        assertEquals(TransactionStatus.IDLE, connection.lastTransactionStatus);
    }

    @Test
    void testQueryCancellation()
    {
        if (isCockroach)
            return;
        RawFrontendMessage query = buildQueryMessage("SELECT pg_sleep(10)");
        connection.sendMessage(query);
        Thread.sleep(msecs(50));
        connection.cancelRequest();
        PollResult pr = connection.pollMessages((con, msg)
            {
                if (msg.type == BackendMessageType.ErrorResponse)
                {
                    NoticeOrError error = NoticeOrError.parse(msg.data);
                    assertEquals("57014", error.code[]);
                    assertEquals("canceling statement due to user request", error.message);
                    return PollAction.BREAK;
                }
                return PollAction.CONTINUE;
            });
        assertEquals(PollResult.POLL_CALLBACK_BREAK, pr);
    }

    @Test
    void testUnprovokedQueryCancellation()
    {
        connection.cancelRequest();
    }

    @Test
    void testExtendedQueryUnnamed()
    {
        int[] paramTypes = [KnownTypeOID.INT, KnownTypeOID.REAL,
            KnownTypeOID.BIGINT, KnownTypeOID.UUID, KnownTypeOID.TEXT];
        int[] returnTypes = paramTypes.dup;
        if (isCockroach)
        {
            // cockroachdb integer is bigint, and read->double is unexplainable.
            returnTypes[0] = KnownTypeOID.BIGINT;
            returnTypes[1] = KnownTypeOID.DOUBLE_PRECISION;
        }
        RawFrontendMessage fmsg = buildParseMessage(
            "", "SELECT $1::integer, $2::real, $3::bigint, $4::uuid, $5::text", paramTypes);
        connection.sendMessage(fmsg);
        connection.sendMessage(buildFlushMessage());

        PollCallback poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.ParseComplete:
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };

        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));

        connection.sendMessage(buildDescribeMessage(
            PreparedStatementOrPortal.PREPARED_STATEMENT, ""));
        connection.sendMessage(buildFlushMessage());

        ParameterDescription paramDescr;
        RowDescription rowDescr;

        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.ParameterDescription:
                        paramDescr = ParameterDescription.parse(msg.data);
                        return PollAction.CONTINUE;
                    case BackendMessageType.RowDescription:
                        rowDescr = RowDescription.parse(msg.data);
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };

        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));
        assertEquals(paramTypes, paramDescr.paramTypeOIDs);
        assertEquals(returnTypes, rowDescr.fieldDescriptions.map!(fd => fd.type).array);

        int p1 = 42;
        float p2 = float.infinity;
        long p3 = long.max;
        UUID p4 = UUID("beefb950-6c85-4ec6-b448-4ab38fa40825");
        string p5 = "some string с юникодом";

        fmsg = buildBindMessage("", "",
            [BindParam(&p1, FormatCode.BINARY,
                cast(FieldSerializingFunction) &serializePrimitiveFieldBinary!int),
             BindParam(&p2, FormatCode.BINARY,
                cast(FieldSerializingFunction) &serializePrimitiveFieldBinary!float),
             BindParam(&p3, FormatCode.BINARY,
                cast(FieldSerializingFunction) &serializePrimitiveFieldBinary!long),
             BindParam(&p4, FormatCode.BINARY,
                cast(FieldSerializingFunction) &serializeUUIDFieldBinary),
             BindParam(&p5, FormatCode.BINARY,
                cast(FieldSerializingFunction) &serializeByteArrayField)],
             [FormatCode.BINARY]);

        connection.sendMessage(fmsg);
        connection.sendMessage(buildExecuteMessage(""));
        connection.sendMessage(buildSyncMessage());

        DataRow[] rows;
        bool bindCompleteReceived;
        CommandComplete comCmpl;

        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.BindComplete:
                        bindCompleteReceived = true;
                        return PollAction.CONTINUE;
                    case BackendMessageType.DataRow:
                        rows ~= DataRow.parse(msg.data);
                        return PollAction.CONTINUE;
                    case BackendMessageType.CommandComplete:
                        comCmpl = CommandComplete.parse(msg.data);
                        return PollAction.CONTINUE;
                    default:
                        return PollAction.CONTINUE;
                }
            };

        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(poller));
        assertTrue(bindCompleteReceived);
        assertEquals(1, rows.length);
        assertEquals("SELECT 1", comCmpl.commandTag);

        int r1;
        float r2;
        long r3;
        UUID r4;
        string r5;

        if (!isCockroach)
        {
            // postgres
            deserializePrimitiveField!(int, FormatCode.BINARY)(
                rows[0].columns[0].isNull, rows[0].columns[0].value, &r1);
            deserializePrimitiveField!(float, FormatCode.BINARY)(
                rows[0].columns[1].isNull, rows[0].columns[1].value, &r2);
        }
        else
        {
            long r1cdb;
            deserializePrimitiveField!(long, FormatCode.BINARY)(
                rows[0].columns[0].isNull, rows[0].columns[0].value, &r1cdb);
            r1 = r1cdb.to!int;

            double r2cdb;
            deserializePrimitiveField!(double, FormatCode.BINARY)(
                rows[0].columns[1].isNull, rows[0].columns[1].value, &r2cdb);
            r2 = r2cdb.to!float;
        }
        deserializePrimitiveField!(long, FormatCode.BINARY)(
            rows[0].columns[2].isNull, rows[0].columns[2].value, &r3);
        deserializeUUIDField!(FormatCode.BINARY)(
            rows[0].columns[3].isNull, rows[0].columns[3].value, &r4);
        deserializeByteArrayField(
            rows[0].columns[4].isNull, rows[0].columns[4].value, cast(ubyte[]*) &r5);

        assertEquals(p1, r1);
        assertEquals(p2, r2);
        assertEquals(p3, r3);
        assertEquals(p4, r4);
        assertEquals(p5, r5);
    }

    @Test
    void testListenNotify()
    {
        if (isCockroach)
            return;
        DebugPSQLConnection connection2;
        ITransport transport2;
        transport2 = transport.duplicate();
        connection2 = new DebugPSQLConnection(transport2, SSLPolicy.NEVER);
        scope(exit) connection2.close();
        connection2.handshakeAndAuthenticate(startupParams, password);

        RawFrontendMessage query = buildQueryMessage("LISTEN channel");
        connection.sendMessage(query);
        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(null));

        query = buildQueryMessage("NOTIFY channel, 'payload'");
        connection2.sendMessage(query);

        connection.notificationCallback =
            (PSQLConnection receiver, NotificationResponse message)
            {
                assertEquals(connection2.backendKeyData.processId, message.procId);
                assertEquals("channel", message.channel);
                assertEquals("payload", message.payload);
                return PollAction.BREAK;
            };

        assertEquals(PollResult.RFQ_RECEIVED, connection2.pollMessages(null));
        assertEquals(
            PollResult.NOTIFICATION_CALLBACK_BREAK,
            connection.pollMessages(null));
    }

    @Test
    void testCopyMode()
    {
        if (isCockroach)
            return;

        RawFrontendMessage query = buildQueryMessage(
            "CREATE TABLE IF NOT EXISTS raw_ints(row1 integer);
             DELETE FROM raw_ints;
             COPY raw_ints (row1) FROM STDIN;");
        connection.sendMessage(query);

        PollCallback poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.CopyInResponse:
                        CopyResponse cr = CopyResponse.parse(msg.data);
                        assertEquals(0, cr.overallFormat);
                        assertEquals([short(0)], cr.formatCodes);
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };
        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));

        for (int i = 0; i < 50; i++)
            connection.sendMessage(
                buildCopyDataMessage(cast(ubyte[]) (i.to!string ~ "\n")));
        connection.sendMessage(buildCopyDoneMessage());

        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.CommandComplete:
                        CommandComplete cc = CommandComplete.parse(msg.data);
                        assertEquals("COPY 50", cc.commandTag);
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };

        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));
        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(null));

        query = buildQueryMessage("
            COPY raw_ints (row1) TO STDOUT");
        connection.sendMessage(query);

        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.CopyOutResponse:
                        CopyResponse cr = CopyResponse.parse(msg.data);
                        assertEquals(0, cr.overallFormat);
                        assertEquals([cast(short) FormatCode.TEXT], cr.formatCodes);
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };

        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));

        CopyData[] rows;
        poller =
            (PSQLConnection con, RawBackendMessage msg)
            {
                switch (msg.type)
                {
                    case BackendMessageType.CopyData:
                        CopyData cd = CopyData.parse(msg.data);
                        rows ~= cd;
                        return PollAction.CONTINUE;
                    case BackendMessageType.CopyDone:
                        return PollAction.BREAK;
                    default:
                        return PollAction.CONTINUE;
                }
            };
        assertEquals(PollResult.POLL_CALLBACK_BREAK, connection.pollMessages(poller));
        assertEquals(50, rows.length);
        assertEquals(PollResult.RFQ_RECEIVED, connection.pollMessages(null));

        for (int i = 0; i < 50; i++)
            assertEquals(i, (cast(string) rows[i].data).strip.to!int);
    }

}

mixin Main;