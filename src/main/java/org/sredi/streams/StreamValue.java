package org.sredi.streams;

import java.util.Arrays;
import java.util.Objects;

import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class StreamValue {
    private final StreamId itemId;
    private final RespValue[] values;

    public StreamValue(StreamId itemId, RespValue[] values) {
        this.itemId = itemId;
        this.values = values;
    }

    public StreamId getItemId() {
        return itemId;
    }

    public RespValue[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "StreamValue [itemId=" + itemId + ", values=" + Arrays.toString(values) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(values);
        result = prime * result + Objects.hash(itemId);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof StreamValue))
            return false;
        StreamValue other = (StreamValue) obj;
        return Objects.equals(itemId, other.itemId) && Arrays.equals(values, other.values);
    }

    public RespArrayValue asRespArrayValue() {
        return new RespArrayValue(new RespValue[] {
                new RespBulkString(itemId.getId().getBytes()),
                new RespArrayValue(values)
        });
    }

}
