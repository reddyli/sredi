package org.sredi.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.storage.StoredData;

public class RdbFileParser {
    private static final Logger log = LoggerFactory.getLogger(RdbFileParser.class);

    private final RdbParsePrimitives reader;
    private Clock clock;

    public RdbFileParser(BufferedInputStream file, Clock clock) {
        reader = new RdbParsePrimitives(file);
        this.clock = clock;
    }

    public OpCode initDB() throws IOException {
        reader.readHeader();
        String dbVersion = new String(reader.readChars(4));
        log.info("DB Version: {}", dbVersion);
        return reader.readCode();
    }

    public OpCode selectDB(Map<String, StoredData> dbData) throws IOException {
        int dbNumber = reader.readValue(reader.read()).getValue();
        log.info("Select DB: {}", dbNumber);
        int next = reader.read();
        OpCode nextCode = OpCode.fromCode(next);
        if (nextCode == OpCode.RESIZEDB) {
            skipResizeDb();
            next = reader.read(); // read the valueType or next code
            nextCode = OpCode.fromCode(next);
        }
        while (nextCode == null
                || nextCode == OpCode.EXPIRETIME
                || nextCode == OpCode.EXPIRETIMEMS) {
            log.debug("Next code at file index: {} {}", nextCode, reader.file.readCount);
            Long expiryTime = null;
            if (nextCode == OpCode.EXPIRETIME) {
                // read 4 bytes for expiry time in seconds
                expiryTime = reader.readIntLittleEnd() * 1000L;
                next = reader.read();
            } else if (nextCode == OpCode.EXPIRETIMEMS) {
                // read 8 bytes for expiry time in milliseconds
                expiryTime = reader.readLongLittleEnd();
                next = reader.read();
            }

            int valueType = next;
            next = reader.read();
            // 0 = String Encoding
            // 1 = List Encoding
            // 2 = Set Encoding
            // 3 = Sorted Set Encoding
            // 4 = Hash Encoding
            // 9 = Zipmap Encoding
            // 10 = Ziplist Encoding
            // 11 = Intset Encoding
            // 12 = Sorted Set in Ziplist Encoding
            // 13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
            // 14 = List in Quicklist encoding (Introduced in RDB version 7)
            EncodedValue keyLength = reader.readValue(next);
            byte[] keyBytes;
            if (!keyLength.isString()) {
                // length encoded string
                keyBytes = reader.readNBytes(keyLength.getValue());
            } else {
                keyBytes = keyLength.getString().getBytes();
            }
            log.debug("Read key: {} with expiry: {}", new String(keyBytes), expiryTime);
            next = reader.read();
            EncodedValue value = reader.readValue(next);
            if (valueType != 0 || !value.isInt()) {
                throw new IllegalArgumentException(String.format(
                        "Expected value should be a length prefix string, valueType: %d, value: %s",
                        valueType,
                        value));
            }
            byte[] valueBytes = reader.readNBytes(value.getValue());
            long now = clock.millis();
            Long ttlMillis = null;
            if (expiryTime != null) {
                ttlMillis = expiryTime - now;
            }
            // write it only if no expiration or expiration is not already past
            if (ttlMillis == null || ttlMillis > 0L) {
                StoredData valueData = new StoredData(valueBytes, now, ttlMillis);
                dbData.put(new String(keyBytes), valueData);
            } else {
                log.debug("Skipping expired key: {}", new String(keyBytes));
            }

            next = reader.read();
            nextCode = OpCode.fromCode(next);
        }
        return nextCode;
    }

    public OpCode skipAux() throws IOException {
        OpCode nextCode = null;
        while (nextCode == null || nextCode == OpCode.AUX) {
            nextCode = OpCode.fromCode(reader.read());
        }
        return nextCode;
    }

    public void skipResizeDb() throws IOException {
        EncodedValue dbSize = reader.readValue(reader.read());
        EncodedValue expirySize = reader.readValue(reader.read());
        log.debug("Skipping RESIZEDB dbSize: {}, expirySize: {}", dbSize.getValue(), expirySize.getValue());
    }
}