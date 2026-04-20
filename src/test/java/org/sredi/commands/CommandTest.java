package org.sredi.commands;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespConstants;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.storage.Orchestrator;
import org.sredi.storage.DataEntry;

@ExtendWith(MockitoExtension.class)
class CommandTest {

    @Mock
    private Orchestrator mockOrchestrator;

    // Helper to create RespBulkString from string
    private RespBulkString bulkString(String value) {
        return new RespBulkString(value.getBytes());
    }

    @Nested
    class GetCommandTests {

        @Test
        void getExistingKey() {
            // Setup: key "mykey" exists with value "myvalue"
            when(mockOrchestrator.containsKey("mykey")).thenReturn(true);
            when(mockOrchestrator.get("mykey")).thenReturn(
                new DataEntry("myvalue".getBytes(), 0L, null)
            );
            when(mockOrchestrator.isExpired(any())).thenReturn(false);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("mykey"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return the value as bulk string
            String response = new String(result);
            assertTrue(response.contains("myvalue"));
        }

        @Test
        void getNonExistentKey() {
            // Setup: key doesn't exist
            when(mockOrchestrator.containsKey("nokey")).thenReturn(false);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("nokey"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return NULL
            assertArrayEquals(RespConstants.NULL, result);
        }

        @Test
        void getExpiredKey() {
            // Setup: key exists but is expired
            DataEntry expiredData = new DataEntry("oldvalue".getBytes(), 0L, 1000L);
            when(mockOrchestrator.containsKey("expiredkey")).thenReturn(true);
            when(mockOrchestrator.get("expiredkey")).thenReturn(expiredData);
            when(mockOrchestrator.isExpired(expiredData)).thenReturn(true);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("expiredkey"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return NULL and delete the key
            assertArrayEquals(RespConstants.NULL, result);
            verify(mockOrchestrator).delete("expiredkey");
        }
    }

    @Nested
    class SetCommandTests {

        @Test
        void setBasic() {
            // Setup
            when(mockOrchestrator.getCurrentTime()).thenReturn(1000L);

            // Execute
            SetCommand cmd = new SetCommand(bulkString("key1"), bulkString("value1"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return OK and call set()
            assertArrayEquals(RespConstants.OK, result);
            verify(mockOrchestrator).set(eq("key1"), any(DataEntry.class));
        }

        @Test
        void setWithNxWhenKeyDoesNotExist() {
            // Setup: key doesn't exist
            when(mockOrchestrator.getCurrentTime()).thenReturn(1000L);
            when(mockOrchestrator.containsUnexpiredKey("newkey")).thenReturn(false);

            // Execute: SET newkey value NX
            SetCommand cmd = new SetCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("SET"),
                bulkString("newkey"),
                bulkString("value"),
                bulkString("nx")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should succeed
            assertArrayEquals(RespConstants.OK, result);
            verify(mockOrchestrator).set(eq("newkey"), any(DataEntry.class));
        }

        @Test
        void setWithNxWhenKeyExists() {
            // Setup: key already exists
            when(mockOrchestrator.containsUnexpiredKey("existingkey")).thenReturn(true);

            // Execute: SET existingkey value NX
            SetCommand cmd = new SetCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("SET"),
                bulkString("existingkey"),
                bulkString("value"),
                bulkString("nx")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return NULL (key not set)
            assertArrayEquals(RespConstants.NULL, result);
            verify(mockOrchestrator, never()).set(anyString(), any(DataEntry.class));
        }
    }

    @Nested
    class IncrCommandTests {

        @Test
        void incrExistingNumericKey() {
            // Setup: key exists with value "10"
            when(mockOrchestrator.containsKey("counter")).thenReturn(true);
            when(mockOrchestrator.get("counter")).thenReturn(
                new DataEntry("10".getBytes(), 0L, null)
            );
            when(mockOrchestrator.getCurrentTime()).thenReturn(1000L);

            // Execute
            IncrCommand cmd = new IncrCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("INCR"),
                bulkString("counter")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return 11
            String response = new String(result);
            assertTrue(response.contains("11"));
        }

        @Test
        void incrNonExistentKey() {
            // Setup: key doesn't exist
            when(mockOrchestrator.containsKey("newcounter")).thenReturn(false);
            when(mockOrchestrator.getCurrentTime()).thenReturn(1000L);

            // Execute
            IncrCommand cmd = new IncrCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("INCR"),
                bulkString("newcounter")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return 0 (creates key with value 0)
            String response = new String(result);
            assertTrue(response.contains("0"));
            verify(mockOrchestrator).set(eq("newcounter"), any(DataEntry.class));
        }
    }

    @Nested
    class KeysCommandTests {

        @Test
        void keysReturnsAllKeys() {
            // Setup: orchestrator has 3 keys
            when(mockOrchestrator.getKeys()).thenReturn(Set.of("key1", "key2", "key3"));

            // Execute
            KeysCommand cmd = new KeysCommand("*");
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: response should contain all keys
            String response = new String(result);
            assertTrue(response.contains("key1"));
            assertTrue(response.contains("key2"));
            assertTrue(response.contains("key3"));
        }

        @Test
        void keysReturnsEmptyArray() {
            // Setup: orchestrator has no keys
            when(mockOrchestrator.getKeys()).thenReturn(Set.of());

            // Execute
            KeysCommand cmd = new KeysCommand("*");
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify: should return empty array "*0\r\n"
            assertEquals("*0\r\n", new String(result));
        }
    }

    @Nested
    class TypeCommandTests {

        @Test
        void typeReturnsStringForStringKey() {
            // Setup
            when(mockOrchestrator.getType("mykey")).thenReturn(
                new RespSimpleStringValue("string")
            );

            // Execute
            TypeCommand cmd = new TypeCommand(bulkString("mykey"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify
            assertEquals("+string\r\n", new String(result));
        }

        @Test
        void typeReturnsNoneForNonExistentKey() {
            // Setup
            when(mockOrchestrator.getType("nokey")).thenReturn(
                new RespSimpleStringValue("none")
            );

            // Execute
            TypeCommand cmd = new TypeCommand(bulkString("nokey"));
            byte[] result = cmd.execute(mockOrchestrator);

            // Verify
            assertEquals("+none\r\n", new String(result));
        }
    }

    @Nested
    class ListCommandTests {

        @Test
        void lpushReturnsListSize() {
            when(mockOrchestrator.lpush("mylist", "hello")).thenReturn(1L);

            LPushCommand cmd = new LPushCommand();
            cmd.setArgs(new RespValue[] { bulkString("LPUSH"), bulkString("mylist"), bulkString("hello") });
            byte[] result = cmd.execute(mockOrchestrator);

            assertEquals(":1\r\n", new String(result));
        }

        @Test
        void rpushReturnsListSize() {
            when(mockOrchestrator.rpush("mylist", "world")).thenReturn(2L);

            RPushCommand cmd = new RPushCommand();
            cmd.setArgs(new RespValue[] { bulkString("RPUSH"), bulkString("mylist"), bulkString("world") });
            byte[] result = cmd.execute(mockOrchestrator);

            assertEquals(":2\r\n", new String(result));
        }

        @Test
        void lpopReturnsValue() {
            when(mockOrchestrator.lpop("mylist")).thenReturn("hello");

            LPopCommand cmd = new LPopCommand();
            cmd.setArgs(new RespValue[] { bulkString("LPOP"), bulkString("mylist") });
            byte[] result = cmd.execute(mockOrchestrator);

            String response = new String(result);
            assertTrue(response.contains("hello"));
        }

        @Test
        void lpopReturnsNullForMissingKey() {
            when(mockOrchestrator.lpop("nokey")).thenReturn(null);

            LPopCommand cmd = new LPopCommand();
            cmd.setArgs(new RespValue[] { bulkString("LPOP"), bulkString("nokey") });
            byte[] result = cmd.execute(mockOrchestrator);

            assertArrayEquals(RespConstants.NULL, result);
        }

        @Test
        void rpopReturnsValue() {
            when(mockOrchestrator.rpop("mylist")).thenReturn("world");

            RPopCommand cmd = new RPopCommand();
            cmd.setArgs(new RespValue[] { bulkString("RPOP"), bulkString("mylist") });
            byte[] result = cmd.execute(mockOrchestrator);

            String response = new String(result);
            assertTrue(response.contains("world"));
        }

        @Test
        void rpopReturnsNullForMissingKey() {
            when(mockOrchestrator.rpop("nokey")).thenReturn(null);

            RPopCommand cmd = new RPopCommand();
            cmd.setArgs(new RespValue[] { bulkString("RPOP"), bulkString("nokey") });
            byte[] result = cmd.execute(mockOrchestrator);

            assertArrayEquals(RespConstants.NULL, result);
        }

        @Test
        void lrangeReturnsElements() {
            when(mockOrchestrator.lrange("mylist", 0, -1)).thenReturn(List.of("a", "b", "c"));

            LRangeCommand cmd = new LRangeCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("LRANGE"), bulkString("mylist"), bulkString("0"), bulkString("-1")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            String response = new String(result);
            assertTrue(response.startsWith("*3"));
            assertTrue(response.contains("a"));
            assertTrue(response.contains("b"));
            assertTrue(response.contains("c"));
        }

        @Test
        void lrangeReturnsEmptyForMissingKey() {
            when(mockOrchestrator.lrange("nokey", 0, -1)).thenReturn(List.of());

            LRangeCommand cmd = new LRangeCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("LRANGE"), bulkString("nokey"), bulkString("0"), bulkString("-1")
            });
            byte[] result = cmd.execute(mockOrchestrator);

            assertEquals("*0\r\n", new String(result));
        }
    }
}

