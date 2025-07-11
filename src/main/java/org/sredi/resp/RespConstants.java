package org.sredi.resp;

public class RespConstants {
    
    public static final byte[] NULL = "$-1\r\n".getBytes();
    public static final byte[] OK = "+OK\r\n".getBytes();
    public static final byte[] CRLF = "\r\n".getBytes();

    public static final RespValue NULL_VALUE = RespNullValue.INSTANCE;
}
