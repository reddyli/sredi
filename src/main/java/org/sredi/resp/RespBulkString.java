package org.sredi.resp;

import java.io.IOException;
import java.lang.Math;
import java.util.Arrays;

import org.sredi.io.BufferedInputLineReader;

public class RespBulkString extends RespValueBase {
    private final byte[] value;

    public RespBulkString(byte[] value) {
        super(RespType.BULK_STRING);
        this.value = value;
    }

    public RespBulkString(BufferedInputLineReader reader) throws IOException {
        super(RespType.BULK_STRING);
        int len = reader.readInt();
        if (len >= 0) {
            value = reader.readNBytes(len);
            reader.readCRLF();
        } else {
            value = null;
        }
    }

    public byte[] asResponse() {
        return asResponse(true);
    }

    public byte[] asResponse(boolean trailingCRLF) {
        if (isNullValue()) {
            return RespConstants.NULL;
        }

        StringBuilder builder = new StringBuilder("$").append(value.length).append("\r\n");
        byte[] prefixBytes = builder.toString().getBytes();
        int n = prefixBytes.length + value.length;
        if (trailingCRLF) {
            n += 2;
        }

        byte[] result = new byte[n];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(value, 0, result, prefixBytes.length, value.length);
        if (trailingCRLF) {
            result[n - 2] = '\r';
            result[n - 1] = '\n';
        }
        return result;
    }

    @Override
    public String toString() {
        if (isNullValue()) {
            return RespNullValue.INSTANCE.toString();
        }
        return "BulkString [length=" + value.length + ", value=" + truncValueString(15) + "]";
    }

    private String truncValueString(int trunclen) {
        trunclen = Math.min(value.length, trunclen);
        StringBuilder sb = new StringBuilder(new String(value, 0, trunclen));
        if (value.length > trunclen) {
            sb.append("...");
        }
        return sb.toString();
    }

    public boolean isNullValue() {
        return value == null;
    }

    public byte[] getValue() {
        return !isNullValue() ? value : RespConstants.NULL;
    }

    public String getValueAsString() {
        return new String(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass()) {
            return false;
        }
        RespBulkString other = (RespBulkString) obj;
        return Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

}
