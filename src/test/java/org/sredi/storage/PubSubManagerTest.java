package org.sredi.storage;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sredi.replication.ClientConnection;

class PubSubManagerTest {

    private AtomicReference<ClientConnection> currentConn;
    private PubSubManager manager;
    private ClientConnection connA;
    private ClientConnection connB;

    @BeforeEach
    void setUp() {
        currentConn = new AtomicReference<>();
        manager = new PubSubManager(currentConn::get);
        connA = mock(ClientConnection.class);
        connB = mock(ClientConnection.class);
    }

    @Test
    void subscribeReturnsRunningCount() {
        currentConn.set(connA);
        assertEquals(1, manager.subscribe("news"));
        assertEquals(2, manager.subscribe("weather"));
        assertEquals(2, manager.subscribe("news"), "re-subscribe should not double-count");
        assertTrue(manager.isSubscribed(connA));
    }

    @Test
    void unsubscribeDecrementsAndCleansUp() {
        currentConn.set(connA);
        manager.subscribe("news");
        manager.subscribe("weather");

        assertEquals(1, manager.unsubscribe("news"));
        assertEquals(0, manager.unsubscribe("weather"));
        assertFalse(manager.isSubscribed(connA));
        assertEquals(0, manager.unsubscribe("ghost"), "unsubscribe from non-subscribed channel");
    }

    @Test
    void unsubscribeAllReturnsChannelsAndClears() {
        currentConn.set(connA);
        manager.subscribe("a");
        manager.subscribe("b");
        manager.subscribe("c");

        List<String> removed = manager.unsubscribeAll();
        assertEquals(3, removed.size());
        assertTrue(removed.containsAll(List.of("a", "b", "c")));
        assertFalse(manager.isSubscribed(connA));
        assertTrue(manager.unsubscribeAll().isEmpty(), "second call is a no-op");
    }

    @Test
    void publishDeliversToSubscribersAndReturnsCount() throws IOException {
        currentConn.set(connA);
        manager.subscribe("news");
        currentConn.set(connB);
        manager.subscribe("news");
        when(connA.isClosed()).thenReturn(false);
        when(connB.isClosed()).thenReturn(false);

        int delivered = manager.publish("news", "hello".getBytes());

        assertEquals(2, delivered);
        verify(connA).writeFlush(any(byte[].class));
        verify(connB).writeFlush(any(byte[].class));
    }

    @Test
    void publishPayloadIsRespMessageArray() throws IOException {
        currentConn.set(connA);
        manager.subscribe("news");
        when(connA.isClosed()).thenReturn(false);

        manager.publish("news", "hi".getBytes());

        verify(connA).writeFlush(argThat(bytes -> {
            String s = new String(bytes);
            return s.startsWith("*3\r\n")
                    && s.contains("$7\r\nmessage\r\n")
                    && s.contains("$4\r\nnews\r\n")
                    && s.contains("$2\r\nhi\r\n");
        }));
    }

    @Test
    void publishReturnsZeroWhenNoSubscribers() {
        assertEquals(0, manager.publish("ghost", "x".getBytes()));
    }

    @Test
    void publishSkipsClosedConnections() throws IOException {
        currentConn.set(connA);
        manager.subscribe("news");
        currentConn.set(connB);
        manager.subscribe("news");
        when(connA.isClosed()).thenReturn(true);
        when(connB.isClosed()).thenReturn(false);

        assertEquals(1, manager.publish("news", "hi".getBytes()));
        verify(connA, never()).writeFlush(any());
        verify(connB).writeFlush(any());
    }

    @Test
    void publishCountsDeliveryFailuresAsNotDelivered() throws IOException {
        currentConn.set(connA);
        manager.subscribe("news");
        currentConn.set(connB);
        manager.subscribe("news");
        when(connA.isClosed()).thenReturn(false);
        when(connB.isClosed()).thenReturn(false);
        doThrow(new IOException("broken pipe")).when(connA).writeFlush(any());

        assertEquals(1, manager.publish("news", "hi".getBytes()));
    }

    @Test
    void removeConnectionCleansUpBothMaps() throws IOException {
        currentConn.set(connA);
        manager.subscribe("news");
        manager.subscribe("weather");
        currentConn.set(connB);
        manager.subscribe("news");

        manager.removeConnection(connA);

        assertFalse(manager.isSubscribed(connA));
        when(connB.isClosed()).thenReturn(false);
        assertEquals(1, manager.publish("news", "x".getBytes()),
                "news should only deliver to connB");
        assertEquals(0, manager.publish("weather", "x".getBytes()),
                "weather channel should be gone entirely");
    }
}
