package org.sredi.rdb;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RdbParseTest {

    @Nested
    class OpCodeTests {

        @Test
        void fromCodeReturnsEOF() {
            OpCode code = OpCode.fromCode(0xFF);
            assertEquals(OpCode.EOF, code);
        }

        @Test
        void fromCodeReturnsSELECTDB() {
            OpCode code = OpCode.fromCode(0xFE);
            assertEquals(OpCode.SELECTDB, code);
        }

        @Test
        void fromCodeReturnsEXPIRETIME() {
            OpCode code = OpCode.fromCode(0xFD);
            assertEquals(OpCode.EXPIRETIME, code);
        }

        @Test
        void fromCodeReturnsEXPIRETIMEMS() {
            OpCode code = OpCode.fromCode(0xFC);
            assertEquals(OpCode.EXPIRETIMEMS, code);
        }

        @Test
        void fromCodeReturnsRESIZEDB() {
            OpCode code = OpCode.fromCode(0xFB);
            assertEquals(OpCode.RESIZEDB, code);
        }

        @Test
        void fromCodeReturnsAUX() {
            OpCode code = OpCode.fromCode(0xFA);
            assertEquals(OpCode.AUX, code);
        }

        @Test
        void fromCodeReturnsNullForUnknown() {
            OpCode code = OpCode.fromCode(0x00);
            assertNull(code);
        }
    }

    @Nested
    class ByteUtilsTests {

        @Test
        void decodeIntBigEndian() {
            // 0x01 0x02 0x03 0x04 = 16909060 in big-endian
            byte[] bytes = { 0x01, 0x02, 0x03, 0x04 };
            int result = ByteUtils.decodeInt(bytes);
            assertEquals(16909060, result);
        }

        @Test
        void decodeIntTwoBytes() {
            // high=0x01, low=0x02 = 258
            int result = ByteUtils.decodeInt(0x01, 0x02);
            assertEquals(258, result);
        }

        @Test
        void decodeIntLittleEndian() {
            // 0x04 0x03 0x02 0x01 = 16909060 in little-endian
            byte[] bytes = { 0x04, 0x03, 0x02, 0x01 };
            int result = ByteUtils.decodeIntLittleEnd(bytes);
            assertEquals(16909060, result);
        }

        @Test
        void decodeLongLittleEndian() {
            // Simple case: 0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00 = 1
            byte[] bytes = { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            long result = ByteUtils.decodeLongLittleEnd(bytes);
            assertEquals(1L, result);
        }

        @Test
        void compareToStringMatches() {
            char[] chars = { 'S', 'R', 'E', 'D', 'I' };
            assertTrue(ByteUtils.compareToString(chars, "SREDI"));
        }

        @Test
        void compareToStringDoesNotMatch() {
            char[] chars = { 'R', 'E', 'D', 'I', 'S' };
            assertFalse(ByteUtils.compareToString(chars, "SREDI"));
        }
    }

    @Nested
    class EncodedValueTests {

        @Test
        void int6ValueIsInt() {
            EncodedValue value = new EncodedValue(EncodedValue.Code.INT6, 42);
            assertTrue(value.isInt());
            assertFalse(value.isString());
            assertEquals(42, value.getValue());
        }

        @Test
        void int14ValueIsInt() {
            EncodedValue value = new EncodedValue(EncodedValue.Code.INT14, 1000);
            assertTrue(value.isInt());
            assertEquals(1000, value.getValue());
        }

        @Test
        void int32ValueIsInt() {
            EncodedValue value = new EncodedValue(EncodedValue.Code.INT32, 100000);
            assertTrue(value.isInt());
            assertEquals(100000, value.getValue());
        }

        @Test
        void intString8ValueIsString() {
            EncodedValue value = new EncodedValue(EncodedValue.Code.INTSTRING8, "hello");
            assertTrue(value.isString());
            assertFalse(value.isInt());
            assertEquals("hello", value.getString());
        }

        @Test
        void getValueThrowsForStringType() {
            EncodedValue value = new EncodedValue(EncodedValue.Code.INTSTRING8, "hello");
            assertThrows(IllegalStateException.class, () -> value.getValue());
        }

        @Test
        void equalsAndHashCode() {
            EncodedValue v1 = new EncodedValue(EncodedValue.Code.INT6, 42);
            EncodedValue v2 = new EncodedValue(EncodedValue.Code.INT6, 42);
            EncodedValue v3 = new EncodedValue(EncodedValue.Code.INT6, 99);

            assertEquals(v1, v2);
            assertEquals(v1.hashCode(), v2.hashCode());
            assertNotEquals(v1, v3);
        }
    }
}

