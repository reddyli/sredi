package org.sredi;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sredi.commands.Command;
import org.sredi.commands.CommandConstructor;
import org.sredi.io.BufferedInputLineReader;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueParser;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.CentralRepository;

class IntegrationTest {

    private RespValueParser parser;
    private CommandConstructor commandConstructor;
    private TestableRepository repository;
    private Clock fixedClock;

    @BeforeEach
    void setUp() {
        parser = new RespValueParser();
        commandConstructor = new CommandConstructor();
        fixedClock = Clock.fixed(Instant.ofEpochMilli(1000000L), ZoneId.of("UTC"));
        repository = new TestableRepository(fixedClock);
    }

    // Helper to parse RESP input
    private RespValue parseResp(String input) throws IOException {
        BufferedInputLineReader reader = new BufferedInputLineReader(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))
        );
        return parser.parse(reader);
    }

    // Helper to execute a RESP command string and return result as string
    private String execute(String respInput) throws IOException {
        RespValue value = parseResp(respInput);
        Command command = commandConstructor.newCommandFromValue(value);
        assertNotNull(command, "Command should not be null for input: " + respInput);
        byte[] result = command.execute(repository);
        return new String(result, StandardCharsets.UTF_8);
    }

    @Test
    void setAndGet() throws IOException {
        // SET mykey myvalue
        String setResult = execute("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        assertEquals("+OK\r\n", setResult);

        // GET mykey
        String getResult = execute("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        assertEquals("$7\r\nmyvalue\r\n", getResult);
    }

    @Test
    void getReturnsNullForMissingKey() throws IOException {
        String result = execute("*2\r\n$3\r\nGET\r\n$11\r\nnonexistent\r\n");
        assertEquals("$-1\r\n", result);
    }

    @Test
    void incrCreatesKeyIfNotExists() throws IOException {
        // INCR counter (key doesn't exist, creates with value 0 and returns 0)
        String result = execute("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assertEquals(":0\r\n", result);

        // INCR counter again (0 + 1 = 1)
        String result2 = execute("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        assertEquals(":1\r\n", result2);
    }

    @Test
    void keysReturnsAllKeys() throws IOException {
        // Set some keys
        execute("*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n");
        execute("*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n");

        // KEYS *
        String result = execute("*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");

        // Result should be an array containing key1 and key2
        assertTrue(result.startsWith("*"));
        assertTrue(result.contains("key1"));
        assertTrue(result.contains("key2"));
    }

    @Test
    void typeReturnsStringForStringKey() throws IOException {
        execute("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nvalue\r\n");

        String result = execute("*2\r\n$4\r\nTYPE\r\n$5\r\nmykey\r\n");
        assertEquals("+string\r\n", result);
    }

    @Test
    void typeReturnsNoneForMissingKey() throws IOException {
        String result = execute("*2\r\n$4\r\nTYPE\r\n$11\r\nnonexistent\r\n");
        assertEquals("+none\r\n", result);
    }

    @Test
    void pingReturnsPong() throws IOException {
        String result = execute("*1\r\n$4\r\nPING\r\n");
        assertEquals("+PONG\r\n", result);
    }

    @Test
    void echoReturnsArgument() throws IOException {
        String result = execute("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        assertEquals("$5\r\nhello\r\n", result);
    }

    @Test
    void setWithNxOnlyWhenKeyDoesNotExist() throws IOException {
        // SET key1 value1 NX - should succeed (key doesn't exist)
        String result1 = execute("*4\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$2\r\nnx\r\n");
        assertEquals("+OK\r\n", result1);

        // SET key1 value2 NX - should fail (key exists)
        String result2 = execute("*4\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue2\r\n$2\r\nnx\r\n");
        assertEquals("$-1\r\n", result2);

        // Verify original value is still there
        String getResult = execute("*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");
        assertEquals("$6\r\nvalue1\r\n", getResult);
    }

    /**
     * Minimal testable repository that doesn't start network services.
     * Extends LeaderService but skips the start() method.
     */
    static class TestableRepository extends org.sredi.replication.LeaderService {

        TestableRepository(Clock clock) {
            super(createTestOptions(), clock);
        }

        private static SetupOptions createTestOptions() {
            SetupOptions options = new SetupOptions();
            options.parseArgs(new String[] {}); // Use defaults
            return options;
        }

        // Don't start network services
        @Override
        public void start() {
            // Skip network initialization for tests
        }
    }
}

