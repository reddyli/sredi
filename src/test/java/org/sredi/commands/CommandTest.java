package org.sredi.commands;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
import org.sredi.storage.CentralRepository;
import org.sredi.storage.StoredData;

@ExtendWith(MockitoExtension.class)
class CommandTest {

    @Mock
    private CentralRepository mockRepository;

    // Helper to create RespBulkString from string
    private RespBulkString bulkString(String value) {
        return new RespBulkString(value.getBytes());
    }

    @Nested
    class GetCommandTests {

        @Test
        void getExistingKey() {
            // Setup: key "mykey" exists with value "myvalue"
            when(mockRepository.containsKey("mykey")).thenReturn(true);
            when(mockRepository.get("mykey")).thenReturn(
                new StoredData("myvalue".getBytes(), 0L, null)
            );
            when(mockRepository.isExpired(any())).thenReturn(false);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("mykey"));
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return the value as bulk string
            String response = new String(result);
            assertTrue(response.contains("myvalue"));
        }

        @Test
        void getNonExistentKey() {
            // Setup: key doesn't exist
            when(mockRepository.containsKey("nokey")).thenReturn(false);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("nokey"));
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return NULL
            assertArrayEquals(RespConstants.NULL, result);
        }

        @Test
        void getExpiredKey() {
            // Setup: key exists but is expired
            StoredData expiredData = new StoredData("oldvalue".getBytes(), 0L, 1000L);
            when(mockRepository.containsKey("expiredkey")).thenReturn(true);
            when(mockRepository.get("expiredkey")).thenReturn(expiredData);
            when(mockRepository.isExpired(expiredData)).thenReturn(true);

            // Execute
            GetCommand cmd = new GetCommand(bulkString("expiredkey"));
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return NULL and delete the key
            assertArrayEquals(RespConstants.NULL, result);
            verify(mockRepository).delete("expiredkey");
        }
    }

    @Nested
    class SetCommandTests {

        @Test
        void setBasic() {
            // Setup
            when(mockRepository.getCurrentTime()).thenReturn(1000L);

            // Execute
            SetCommand cmd = new SetCommand(bulkString("key1"), bulkString("value1"));
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return OK and call set()
            assertArrayEquals(RespConstants.OK, result);
            verify(mockRepository).set(eq("key1"), any(StoredData.class));
        }

        @Test
        void setWithNxWhenKeyDoesNotExist() {
            // Setup: key doesn't exist
            when(mockRepository.getCurrentTime()).thenReturn(1000L);
            when(mockRepository.containsUnexpiredKey("newkey")).thenReturn(false);

            // Execute: SET newkey value NX
            SetCommand cmd = new SetCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("SET"),
                bulkString("newkey"),
                bulkString("value"),
                bulkString("nx")
            });
            byte[] result = cmd.execute(mockRepository);

            // Verify: should succeed
            assertArrayEquals(RespConstants.OK, result);
            verify(mockRepository).set(eq("newkey"), any(StoredData.class));
        }

        @Test
        void setWithNxWhenKeyExists() {
            // Setup: key already exists
            when(mockRepository.containsUnexpiredKey("existingkey")).thenReturn(true);

            // Execute: SET existingkey value NX
            SetCommand cmd = new SetCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("SET"),
                bulkString("existingkey"),
                bulkString("value"),
                bulkString("nx")
            });
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return NULL (key not set)
            assertArrayEquals(RespConstants.NULL, result);
            verify(mockRepository, never()).set(anyString(), any(StoredData.class));
        }
    }

    @Nested
    class IncrCommandTests {

        @Test
        void incrExistingNumericKey() {
            // Setup: key exists with value "10"
            when(mockRepository.containsKey("counter")).thenReturn(true);
            when(mockRepository.get("counter")).thenReturn(
                new StoredData("10".getBytes(), 0L, null)
            );
            when(mockRepository.getCurrentTime()).thenReturn(1000L);

            // Execute
            IncrCommand cmd = new IncrCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("INCR"),
                bulkString("counter")
            });
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return 11
            String response = new String(result);
            assertTrue(response.contains("11"));
        }

        @Test
        void incrNonExistentKey() {
            // Setup: key doesn't exist
            when(mockRepository.containsKey("newcounter")).thenReturn(false);
            when(mockRepository.getCurrentTime()).thenReturn(1000L);

            // Execute
            IncrCommand cmd = new IncrCommand();
            cmd.setArgs(new org.sredi.resp.RespValue[] {
                bulkString("INCR"),
                bulkString("newcounter")
            });
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return 0 (creates key with value 0)
            String response = new String(result);
            assertTrue(response.contains("0"));
            verify(mockRepository).set(eq("newcounter"), any(StoredData.class));
        }
    }

    @Nested
    class KeysCommandTests {

        @Test
        void keysReturnsAllKeys() {
            // Setup: repository has 3 keys
            when(mockRepository.getKeys()).thenReturn(Set.of("key1", "key2", "key3"));

            // Execute
            KeysCommand cmd = new KeysCommand("*");
            byte[] result = cmd.execute(mockRepository);

            // Verify: response should contain all keys
            String response = new String(result);
            assertTrue(response.contains("key1"));
            assertTrue(response.contains("key2"));
            assertTrue(response.contains("key3"));
        }

        @Test
        void keysReturnsEmptyArray() {
            // Setup: repository has no keys
            when(mockRepository.getKeys()).thenReturn(Set.of());

            // Execute
            KeysCommand cmd = new KeysCommand("*");
            byte[] result = cmd.execute(mockRepository);

            // Verify: should return empty array "*0\r\n"
            assertEquals("*0\r\n", new String(result));
        }
    }

    @Nested
    class TypeCommandTests {

        @Test
        void typeReturnsStringForStringKey() {
            // Setup
            when(mockRepository.getType("mykey")).thenReturn(
                new RespSimpleStringValue("string")
            );

            // Execute
            TypeCommand cmd = new TypeCommand(bulkString("mykey"));
            byte[] result = cmd.execute(mockRepository);

            // Verify
            assertEquals("+string\r\n", new String(result));
        }

        @Test
        void typeReturnsNoneForNonExistentKey() {
            // Setup
            when(mockRepository.getType("nokey")).thenReturn(
                new RespSimpleStringValue("none")
            );

            // Execute
            TypeCommand cmd = new TypeCommand(bulkString("nokey"));
            byte[] result = cmd.execute(mockRepository);

            // Verify
            assertEquals("+none\r\n", new String(result));
        }
    }
}

