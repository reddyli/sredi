package org.sredi.storage;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import lombok.Getter;
import org.sredi.streams.StreamData;

@Getter
public class DataEntry {
    private final byte[] value;
    private final StreamData streamValue;
    private final LinkedList<String> listValue;
    private final DataEntryType type;
    private final long storedAt;
    private final Long ttlMillis;

    public DataEntry(List<String> listValue , long storedAt, Long ttlMillis) {
        this.listValue = new LinkedList<>(listValue);
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
        this.streamValue = null;
        this.type = DataEntryType.LIST;
        this.value = null;
    }

    public DataEntry(StreamData streamValue, long storedAt, Long ttlMillis) {
        this.value = null;
        this.streamValue = streamValue;
        this.type = DataEntryType.STREAM;
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
        this.listValue = null;
    }

    public DataEntry(byte[] value, long storedAt, Long ttlMillis) {
        this.value = value;
        streamValue = null;
        this.type = DataEntryType.STRING;
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
        this.listValue = null;
    }

    public boolean isExpired(long currentTimeMillis) {
        return ttlMillis != null && ttlMillis > 0 && (currentTimeMillis - storedAt) > ttlMillis;
    }

    @Override
    public String toString() {
        return "DataEntry [value="
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
        if (!(obj instanceof DataEntry))
            return false;
        DataEntry other = (DataEntry) obj;
        return Arrays.equals(value, other.value) && storedAt == other.storedAt
                && Objects.equals(ttlMillis, other.ttlMillis);
    }

}
