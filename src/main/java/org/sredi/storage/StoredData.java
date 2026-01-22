package org.sredi.storage;

import java.util.Arrays;
import java.util.Objects;

import lombok.Getter;
import org.sredi.streams.StreamData;

@Getter
public class StoredData {
    private final byte[] value;
    private final StreamData streamValue;
    private final StoredDataType type;
    private final long storedAt;
    private final Long ttlMillis;

    public StoredData(StreamData streamValue, long storedAt, Long ttlMillis) {
        this.value = null;
        this.streamValue = streamValue;
        this.type = StoredDataType.STREAM;
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
    }

    public StoredData(byte[] value, long storedAt, Long ttlMillis) {
        this.value = value;
        streamValue = null;
        this.type = StoredDataType.STRING;
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
    }

    public boolean isExpired(long currentTimeMillis) {
        return ttlMillis != null && ttlMillis > 0 && (currentTimeMillis - storedAt) > ttlMillis;
    }

    @Override
    public String toString() {
        return "StoredData [value="
                + (value != null ? new String(value) : streamValue)
                + ", type=" + type.getTypeResponse().getValueAsString()
                + ", storedAt=" + storedAt
                + ", ttlMillis=" + ttlMillis + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + Objects.hash(storedAt, ttlMillis);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof StoredData))
            return false;
        StoredData other = (StoredData) obj;
        return Arrays.equals(value, other.value) && storedAt == other.storedAt
                && Objects.equals(ttlMillis, other.ttlMillis);
    }

}
