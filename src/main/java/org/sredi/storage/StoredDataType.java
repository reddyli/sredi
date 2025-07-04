package org.sredi.storage;

import org.sredi.resp.RespSimpleStringValue;

public enum StoredDataType {
    STRING, STREAM;

    public RespSimpleStringValue getTypeResponse() {
        return new RespSimpleStringValue(this.name().toLowerCase());
    }

}
