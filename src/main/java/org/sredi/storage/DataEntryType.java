package org.sredi.storage;

import org.sredi.resp.RespSimpleStringValue;

public enum DataEntryType {
    STRING, STREAM, LIST;

    public RespSimpleStringValue getTypeResponse() {
        return new RespSimpleStringValue(this.name().toLowerCase());
    }

}
