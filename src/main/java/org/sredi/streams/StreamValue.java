package org.sredi.streams;

import java.util.Arrays;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

// Single stream entry: ID + field/value pairs
public record StreamValue(StreamId itemId, RespValue[] values) {

    // Converts to RESP format: [id, [field1, value1, ...]]
    public RespArrayValue asRespArrayValue() {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(itemId.getId().getBytes()),
                new RespArrayValue(values)
        });
    }

    @Override
    public String toString() {
        return "StreamValue[id=" + itemId + ", values=" + Arrays.toString(values) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof StreamValue other)) return false;
        return itemId.equals(other.itemId) && Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return 31 * itemId.hashCode() + Arrays.hashCode(values);
    }
}
