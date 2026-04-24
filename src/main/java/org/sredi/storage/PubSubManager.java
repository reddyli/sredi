package org.sredi.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.replication.ClientConnection;
import org.sredi.resp.RespArrayValue;
import org.sredi.resp.RespBulkString;
import org.sredi.resp.RespValue;

public class PubSubManager {
    private static final Logger log = LoggerFactory.getLogger(PubSubManager.class);

    @FunctionalInterface
    public interface ConnectionProvider {
        ClientConnection getCurrentConnection();
    }

    private final ConnectionProvider connectionProvider;
    private final Map<String, Set<ClientConnection>> channelSubscribers;
    private final Map<ClientConnection, Set<String>> channelSubscriptions;


    public PubSubManager(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        this.channelSubscribers = new ConcurrentHashMap<>();
        this.channelSubscriptions = new ConcurrentHashMap<>();
    }

    private ClientConnection getCurrentConnection() {
        ClientConnection connection = connectionProvider.getCurrentConnection();
        if(connection == null) {
            throw new IllegalStateException("No connection available");
        }
        return connection;
    }
    
    public int subscribe(String channel) {
        ClientConnection connection = getCurrentConnection();
        channelSubscribers
                .computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet())
                .add(connection);
        Set<String> channels = channelSubscriptions
                .computeIfAbsent(connection, k -> ConcurrentHashMap.newKeySet());
        channels.add(channel);
        return channels.size();
    }

    public int unsubscribe(String channel) {
        ClientConnection connection = getCurrentConnection();
        removeSubscriber(channel, connection);
        Set<String> channels = channelSubscriptions.get(connection);
        if (channels == null) {
            return 0;
        }
        channels.remove(channel);
        if (channels.isEmpty()) {
            channelSubscriptions.remove(connection);
            return 0;
        }
        return channels.size();
    }

    public List<String> unsubscribeAll() {
        ClientConnection connection = getCurrentConnection();
        Set<String> channels = channelSubscriptions.remove(connection);
        if (channels == null) {
            return Collections.emptyList();
        }
        for (String channel : channels) {
            removeSubscriber(channel, connection);
        }
        return new ArrayList<>(channels);
    }

    public int publish(String channel, byte[] message) {
        Set<ClientConnection> subscribers = channelSubscribers.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }
        byte[] payload = buildMessagePayload(channel, message);
        int delivered = 0;
        for (ClientConnection subscriber : subscribers) {
            if (subscriber.isClosed()) {
                continue;
            }
            try {
                synchronized (subscriber) {
                    subscriber.writeFlush(payload);
                }
                delivered++;
            } catch (IOException e) {
                log.warn("Failed to deliver to subscriber {}: {}", subscriber, e.getMessage());
            }
        }
        return delivered;
    }

    public void removeConnection(ClientConnection connection) {
        Set<String> channels = channelSubscriptions.remove(connection);
        if (channels == null) {
            return;
        }
        for (String channel : channels) {
            removeSubscriber(channel, connection);
        }
    }

    public boolean isSubscribed(ClientConnection connection) {
        Set<String> channels = channelSubscriptions.get(connection);
        return channels != null && !channels.isEmpty();
    }

    private void removeSubscriber(String channel, ClientConnection connection) {
        Set<ClientConnection> subscribers = channelSubscribers.get(channel);
        if (subscribers == null) {
            return;
        }
        subscribers.remove(connection);
        if (subscribers.isEmpty()) {
            channelSubscribers.remove(channel);
        }
    }

    private byte[] buildMessagePayload(String channel, byte[] message) {
        RespValue[] parts = new RespValue[] {
                new RespBulkString("message".getBytes()),
                new RespBulkString(channel.getBytes()),
                new RespBulkString(message)
        };
        return new RespArrayValue(parts).asResponse();
    }
}
