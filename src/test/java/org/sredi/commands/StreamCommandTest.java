package org.sredi.commands;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespType;
import org.sredi.resp.RespValue;
import org.sredi.storage.CentralRepository;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamId;
import org.sredi.streams.StreamValue;

@ExtendWith(MockitoExtension.class)
class StreamCommandTest {

    @Mock
    private CentralRepository mockRepository;

    // Helper to create bulk string
    private RespBulkString bulkString(String value) {
        return new RespBulkString(value.getBytes());
    }

    @Nested
    class XaddCommandTests {

        @Test
        void xaddReturnsStreamId() throws IllegalStreamItemIdException {
            // Arrange
            StreamId expectedId = new StreamId(1000L, 0);
            when(mockRepository.xadd(eq("mystream"), eq("1000-0"), any(RespValue[].class)))
                .thenReturn(expectedId);

            // Act
            XaddCommand cmd = new XaddCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("XADD"),
                bulkString("mystream"),
                bulkString("1000-0"),
                bulkString("field1"),
                bulkString("value1")
            });
            byte[] result = cmd.execute(mockRepository);

            // Assert - result should be bulk string "1000-0"
            String resultStr = new String(result);
            assertTrue(resultStr.contains("1000-0"));
            verify(mockRepository).xadd(eq("mystream"), eq("1000-0"), any(RespValue[].class));
        }

        @Test
        void xaddWithInvalidIdReturnsError() throws IllegalStreamItemIdException {
            // Arrange
            when(mockRepository.xadd(eq("mystream"), eq("invalid"), any(RespValue[].class)))
                .thenThrow(new IllegalStreamItemIdException("ERR: bad id format"));

            // Act
            XaddCommand cmd = new XaddCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("XADD"),
                bulkString("mystream"),
                bulkString("invalid"),
                bulkString("field1"),
                bulkString("value1")
            });
            byte[] result = cmd.execute(mockRepository);

            // Assert - result should be error
            String resultStr = new String(result);
            assertTrue(resultStr.startsWith("-"));
        }
    }

    @Nested
    class XrangeCommandTests {

        @Test
        void xrangeReturnsEntries() throws IllegalStreamItemIdException {
            // Arrange
            StreamId id1 = new StreamId(1000L, 0);
            StreamId id2 = new StreamId(1001L, 0);
            RespValue[] values1 = { bulkString("field1"), bulkString("value1") };
            RespValue[] values2 = { bulkString("field2"), bulkString("value2") };
            List<StreamValue> entries = List.of(
                new StreamValue(id1, values1),
                new StreamValue(id2, values2)
            );
            when(mockRepository.xrange("mystream", "-", "+")).thenReturn(entries);

            // Act
            XrangeCommand cmd = new XrangeCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("XRANGE"),
                bulkString("mystream"),
                bulkString("-"),
                bulkString("+")
            });
            byte[] result = cmd.execute(mockRepository);

            // Assert - result should be array
            String resultStr = new String(result);
            assertTrue(resultStr.startsWith("*")); // Array response
            verify(mockRepository).xrange("mystream", "-", "+");
        }

        @Test
        void xrangeReturnsEmptyForNoMatches() throws IllegalStreamItemIdException {
            // Arrange
            when(mockRepository.xrange("mystream", "9999", "9999")).thenReturn(List.of());

            // Act
            XrangeCommand cmd = new XrangeCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("XRANGE"),
                bulkString("mystream"),
                bulkString("9999"),
                bulkString("9999")
            });
            byte[] result = cmd.execute(mockRepository);

            // Assert - empty array
            String resultStr = new String(result);
            assertEquals("*0\r\n", resultStr);
        }
    }

    @Nested
    class XreadCommandTests {

        @Test
        void xreadReturnsEntries() throws IllegalStreamItemIdException {
            // Arrange
            StreamId id = new StreamId(1000L, 0);
            RespValue[] values = { bulkString("field1"), bulkString("value1") };
            List<List<StreamValue>> result = List.of(
                List.of(new StreamValue(id, values))
            );
            when(mockRepository.xread(List.of("mystream"), List.of("0"), null)).thenReturn(result);

            // Act
            XreadCommand cmd = new XreadCommand();
            cmd.setArgs(new RespValue[] {
                bulkString("XREAD"),
                bulkString("streams"),
                bulkString("mystream"),
                bulkString("0")
            });
            byte[] cmdResult = cmd.execute(mockRepository);

            // Assert
            String resultStr = new String(cmdResult);
            assertTrue(resultStr.startsWith("*")); // Array response
            verify(mockRepository).xread(List.of("mystream"), List.of("0"), null);
        }
    }
}

