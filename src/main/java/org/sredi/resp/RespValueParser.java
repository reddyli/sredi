package org.sredi.resp;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.io.BufferedInputLineReader;

public class RespValueParser {
    private static final Logger log = LoggerFactory.getLogger(RespValueParser.class);

    public RespValue parse(BufferedInputLineReader reader) throws IOException {
        int type = reader.read();

        RespType respType = RespType.of((char)type);
        return switch (respType) {
            case SIMPLE_STRING -> new RespSimpleStringValue(reader);
            case SIMPLE_ERROR -> new RespSimpleErrorValue(reader);
            case INTEGER -> new RespInteger(reader);
            case BULK_STRING -> new RespBulkString(reader);
            case ARRAY -> new RespArrayValue(reader, this);
            case null, default -> {
                log.warn("Unknown type: {}", type);
                yield null;
            }
		};
    }
}
