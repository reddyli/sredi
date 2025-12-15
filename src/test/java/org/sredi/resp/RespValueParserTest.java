package org.sredi.resp;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sredi.io.BufferedInputLineReader;

class RespValueParserTest {

    private RespValueParser parser;

    @BeforeEach
    void setUp() {
        parser = new RespValueParser();
    }

    // Helper method to create a reader from a string
    private BufferedInputLineReader readerFrom(String input) {
        return new BufferedInputLineReader(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void parseSimpleString() throws IOException {
        RespValue result = parser.parse(readerFrom("+OK\r\n"));

        assertEquals(RespType.SIMPLE_STRING, result.getType());
        assertEquals("OK", result.getValueAsString());
    }

    @Test
    void parseSimpleStringWithSpaces() throws IOException {
        RespValue result = parser.parse(readerFrom("+Hello World\r\n"));

        assertEquals(RespType.SIMPLE_STRING, result.getType());
        assertEquals("Hello World", result.getValueAsString());
    }

    @Test
    void parseSimpleError() throws IOException {
        RespValue result = parser.parse(readerFrom("-ERR unknown command\r\n"));

        assertEquals(RespType.SIMPLE_ERROR, result.getType());
        assertEquals("ERR unknown command", result.getValueAsString());
    }

    @Test
    void parseInteger() throws IOException {
        RespValue result = parser.parse(readerFrom(":1000\r\n"));

        assertEquals(RespType.INTEGER, result.getType());
        assertEquals("1000", result.getValueAsString());
    }

    @Test
    void parseNegativeInteger() throws IOException {
        RespValue result = parser.parse(readerFrom(":-500\r\n"));

        assertEquals(RespType.INTEGER, result.getType());
        assertEquals("-500", result.getValueAsString());
    }

    @Test
    void parseZero() throws IOException {
        RespValue result = parser.parse(readerFrom(":0\r\n"));

        assertEquals(RespType.INTEGER, result.getType());
        assertEquals("0", result.getValueAsString());
    }

    @Test
    void parseBulkString() throws IOException {
        RespValue result = parser.parse(readerFrom("$5\r\nhello\r\n"));

        assertEquals(RespType.BULK_STRING, result.getType());
        assertEquals("hello", result.getValueAsString());
    }

    @Test
    void parseBulkStringWithSpaces() throws IOException {
        RespValue result = parser.parse(readerFrom("$11\r\nhello world\r\n"));

        assertEquals(RespType.BULK_STRING, result.getType());
        assertEquals("hello world", result.getValueAsString());
    }

    @Test
    void parseEmptyBulkString() throws IOException {
        RespValue result = parser.parse(readerFrom("$0\r\n\r\n"));

        assertEquals(RespType.BULK_STRING, result.getType());
        assertEquals("", result.getValueAsString());
    }

    @Test
    void parseNullBulkString() throws IOException {
        RespValue result = parser.parse(readerFrom("$-1\r\n"));

        assertEquals(RespType.BULK_STRING, result.getType());
        assertTrue(((RespBulkString) result).isNullValue());
    }

    @Test
    void parseEmptyArray() throws IOException {
        RespValue result = parser.parse(readerFrom("*0\r\n"));

        assertEquals(RespType.ARRAY, result.getType());
        assertEquals(0, ((RespArrayValue) result).getSize());
    }

    @Test
    void parseArrayOfBulkStrings() throws IOException {
        // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        RespValue result = parser.parse(readerFrom("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"));

        assertEquals(RespType.ARRAY, result.getType());
        RespArrayValue array = (RespArrayValue) result;
        assertEquals(2, array.getSize());
        assertEquals("foo", array.getValues()[0].getValueAsString());
        assertEquals("bar", array.getValues()[1].getValueAsString());
    }

    @Test
    void parseArrayOfIntegers() throws IOException {
        RespValue result = parser.parse(readerFrom("*3\r\n:1\r\n:2\r\n:3\r\n"));

        assertEquals(RespType.ARRAY, result.getType());
        RespArrayValue array = (RespArrayValue) result;
        assertEquals(3, array.getSize());
        assertEquals("1", array.getValues()[0].getValueAsString());
        assertEquals("2", array.getValues()[1].getValueAsString());
        assertEquals("3", array.getValues()[2].getValueAsString());
    }

    @Test
    void parseMixedArray() throws IOException {
        // Array with: integer, bulk string, simple string
        RespValue result = parser.parse(readerFrom("*3\r\n:100\r\n$5\r\nhello\r\n+OK\r\n"));

        assertEquals(RespType.ARRAY, result.getType());
        RespArrayValue array = (RespArrayValue) result;
        assertEquals(3, array.getSize());
        assertEquals(RespType.INTEGER, array.getValues()[0].getType());
        assertEquals(RespType.BULK_STRING, array.getValues()[1].getType());
        assertEquals(RespType.SIMPLE_STRING, array.getValues()[2].getType());
    }

    @Test
    void roundTripBulkString() throws IOException {
        String original = "$5\r\nhello\r\n";
        RespValue parsed = parser.parse(readerFrom(original));
        byte[] response = parsed.asResponse();

        assertArrayEquals(original.getBytes(StandardCharsets.UTF_8), response);
    }

    @Test
    void roundTripArray() throws IOException {
        String original = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        RespValue parsed = parser.parse(readerFrom(original));
        byte[] response = parsed.asResponse();

        assertArrayEquals(original.getBytes(StandardCharsets.UTF_8), response);
    }
}

