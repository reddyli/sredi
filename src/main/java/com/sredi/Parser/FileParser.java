package com.sredi.Parser;

import com.sredi.Server.ServerCentral;

import java.io.*;
import java.nio.ByteBuffer;

public class FileParser {

    public static void parseRDBFile(File file) throws IOException {
        InputStream in = new FileInputStream(file);
        int b;
        int lengthEncoding;
        int valueType;
        byte[] bytes;
        String key;
        String value;
        // skip
        while ((b = in.read()) != -1) {
            // do nothing
            if (b == 0xFB) {
                getLength(in);
                getLength(in);
                break;
            }
        }
        System.out.println("skipped");
        // value-type

        while( (valueType = in.read()) != -1) {
            if(valueType == 0xFF) {
                break;
            }
            // Key
            lengthEncoding = getLength(in);
            bytes = in.readNBytes(lengthEncoding);
            key = new String(bytes);
            // Value
            lengthEncoding = getLength(in);
            bytes = in.readNBytes(lengthEncoding);
            value = new String(bytes);
            ServerCentral.keyValueStore.put(key, value);
        }

    }

    private static int getLength(InputStream in) throws IOException {
        int length = 0;
        byte b = (byte)in.read();
        switch (b & 0b11000000) {
            case 0 -> {
                length = b & 0b00111111;
            }
            case 128 -> {
                ByteBuffer buffer = ByteBuffer.allocate(2);
                buffer.put((byte) (b & 00111111));
                buffer.put((byte) in.read());
                buffer.rewind();
                length = buffer.getShort();
            }
            case 256 -> {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.put(b);
                buffer.put(in.readNBytes(3));
                buffer.rewind();
                length = buffer.getInt();
            }
            case 384 -> {
                System.out.println("Special format");
            }
        }
        return length;
    }
}
