package org.sredi.streams;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Singleton that coordinates blocking XREAD operations across streams
public final class StreamsWaitManager {
    private static final Logger log = LoggerFactory.getLogger(StreamsWaitManager.class);

    public static final StreamsWaitManager INSTANCE = new StreamsWaitManager();

    // Maps each waiting lock to the set of stream keys it's waiting on
    private final Map<Object, Set<String>> waitingLocks = new ConcurrentHashMap<>();

    private StreamsWaitManager() {}

    // Called when data is added to a stream - wakes up any waiters on that stream
    public void addNotify(String streamKey) {
        waitingLocks.forEach((lock, streamKeys) -> {
            if (streamKeys.contains(streamKey)) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
    }

    // Reads from streams, blocking until data arrives or timeout expires
    public Map<String, List<StreamValue>> readWithWait(
            Map<String, StreamData> streams, Map<String, StreamId> startIds,
            int count, Clock clock, long timeoutMillis) {

        Map<String, List<StreamValue>> result = new HashMap<>();
        int countPerStream = (count == 0) ? StreamData.MAX_READ_COUNT : count;
        int minRequired = (count == 0) ? 1 : count;

        Object lock = new Object();
        waitingLocks.put(lock, streams.keySet());

        synchronized (lock) {
            try {
                long deadline = clock.millis() + timeoutMillis;
                int totalRead = 0;

                while (totalRead < minRequired && (timeoutMillis == 0 || clock.millis() < deadline)) {
                    totalRead = readFromAllStreams(streams, startIds, countPerStream, result);

                    if (totalRead >= minRequired) {
                        return result;
                    }

                    countPerStream = minRequired - totalRead;
                    lock.wait(timeoutMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("readWithWait interrupted");
            } catch (Exception e) {
                log.error("readWithWait error: {}", e.getMessage(), e);
            } finally {
                waitingLocks.remove(lock);
            }
        }
        return result;
    }

    private int readFromAllStreams(Map<String, StreamData> streams, Map<String, StreamId> startIds,
            int countPerStream, Map<String, List<StreamValue>> result) {
        int totalRead = 0;
        for (var entry : streams.entrySet()) {
            String key = entry.getKey();
            List<StreamValue> values = entry.getValue().readNextValues(countPerStream, startIds.get(key));
            result.computeIfAbsent(key, k -> new ArrayList<>()).addAll(values);
            totalRead += values.size();
        }
        return totalRead;
    }
}
