package org.sredi.replication;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.io.BufferedInputLineReader;
import org.sredi.io.BufferedResponseStreamWriter;
import org.sredi.resp.RespSimpleErrorValue;
import org.sredi.resp.RespValue;
import org.sredi.resp.RespValueBase;
import org.sredi.resp.RespValueContext;
import org.sredi.resp.RespValueParser;

/**
 * Wraps a client socket and provides RESP protocol read/write operations.
 * Handles buffered I/O, value parsing, and thread synchronization for blocking commands.
 */
public class ClientConnection {
    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);

    private final Socket clientSocket;
    private final RespValueParser valueParser;
    private final BufferedInputLineReader reader;
    private final BufferedResponseStreamWriter writer;

    public ClientConnection(Socket clientSocket, RespValueParser valueParser) throws IOException {
        this.clientSocket = clientSocket;
        this.valueParser = valueParser;
        this.reader = new BufferedInputLineReader(new BufferedInputStream(clientSocket.getInputStream()));
        this.writer = new BufferedResponseStreamWriter(new BufferedOutputStream(clientSocket.getOutputStream()));
    }

    // Reads and parses a complete RESP value, attaching byte offset context for replication
    public RespValue readValue() throws IOException {
        long startBytesOffset = reader.getNumBytesReceived();
        RespValue value = valueParser.parse(reader);

        long length = reader.getNumBytesReceived() - startBytesOffset;
        RespValueContext context = new RespValueContext(this, startBytesOffset, (int) length);
        ((RespValueBase) value).setContext(context);
        return value;
    }

    // Reads an RDB snapshot from the leader during replication sync
    public byte[] readRDB() throws IOException {
        int marker = reader.read();
        if (marker != '$') {
            throw new IllegalArgumentException("Expected RDB bulk string marker '$', got: " + (char) marker);
        }
        int length = (int) reader.readLong();
        byte[] rdb = new byte[length];
        for (int i = 0; i < length; i++) {
            rdb[i] = (byte) reader.read();
        }
        return rdb;
    }

    // Returns host:port string for logging
    public String getConnectionString() {
        return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
    }

    public boolean isClosed() {
        return clientSocket.isClosed();
    }

    public void close() throws IOException {
        clientSocket.close();
    }

    // Returns number of bytes available to read without blocking
    public int available() throws IOException {
        return reader.available();
    }

    // Returns total bytes received on this connection (for replication offset tracking)
    public long getNumBytesReceived() {
        return reader.getNumBytesReceived();
    }

    // Writes bytes and flushes immediately
    public void writeFlush(byte[] bytes) throws IOException {
        writer.writeFlush(bytes);
    }

    // Wakes up threads waiting for new data (used by ConnectionManager after reading a value)
    public synchronized void notifyNewValueAvailable() {
        notifyAll();
    }

    // Blocks until new data arrives or timeout expires (used by blocking commands like XREAD)
    public synchronized void waitForNewValueAvailable(long timeoutMillis) throws InterruptedException {
        wait(timeoutMillis);
    }

    // Sends an error response to the client
    public void sendError(String message) {
        try {
            writeFlush(new RespSimpleErrorValue(message).asResponse());
        } catch (IOException e) {
            log.error("Failed to send error response '{}': {}", message, e.getMessage(), e);
        }
    }

    // Sends a response to the client if non-empty
    public void sendResponse(byte[] response) throws IOException {
        if (response != null && response.length > 0) {
            writeFlush(response);
        }
    }

    @Override
    public String toString() {
        return clientSocket.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSocket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ClientConnection other)) {
            return false;
        }
        return Objects.equals(clientSocket, other.clientSocket);
    }
}
