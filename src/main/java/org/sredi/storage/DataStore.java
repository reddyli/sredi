package org.sredi.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.resp.RespSimpleStringValue;
import org.sredi.resp.RespValue;
import org.sredi.streams.IllegalStreamItemIdException;
import org.sredi.streams.StreamData;
import org.sredi.streams.StreamId;
import org.sredi.streams.StreamValue;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory key-value data store with LRU eviction and TTL expiration support.
 */
public class DataStore {
    private static final Logger log = LoggerFactory.getLogger(DataStore.class);

    private final Map<String, DataEntry> entries = new ConcurrentHashMap<>();
    private final LRU lru = new LRU();
    private final Clock clock;
    private final int maxKeys;

    public DataStore(Clock clock, int maxKeys) {
        this.clock = clock;
        this.maxKeys = maxKeys;
    }

    // Core operations

    public DataEntry get(String key) {
        lru.logKeyAccess(key);
        return entries.get(key);
    }

    public DataEntry set(String key, DataEntry entry) {
        evictIfNeeded(key);
        lru.logKeyAccess(key);
        return entries.put(key, entry);
    }

    public void delete(String key) {
        entries.remove(key);
        lru.remove(key);
    }

    public boolean containsKey(String key) {
        return entries.containsKey(key);
    }

    public boolean containsUnexpiredKey(String key) {
        DataEntry entry = entries.get(key);
        return entry != null && !isExpired(entry);
    }

    public Collection<String> getKeys() {
        return entries.keySet();
    }

    public RespSimpleStringValue getType(String key) {
        if (entries.containsKey(key)) {
            return entries.get(key).getType().getTypeResponse();
        }
        return new RespSimpleStringValue("none");
    }

    public boolean isExpired(DataEntry entry) {
        return entry.isExpired(clock.millis());
    }

    public long getCurrentTime() {
        return clock.millis();
    }

    // Stream operations

    public StreamId xadd(String key, String itemId, RespValue[] itemMap)
            throws IllegalStreamItemIdException {
        evictIfNeeded(key);
        lru.logKeyAccess(key);
        DataEntry entry = getOrCreateStreamData(key);
        return entry.getStreamValue().add(itemId, clock, itemMap);
    }

    public List<StreamValue> xrange(String key, String start, String end)
            throws IllegalStreamItemIdException {
        lru.logKeyAccess(key);
        DataEntry entry = getOrCreateStreamData(key);
        return entry.getStreamValue().queryRange(start, end);
    }

    public List<List<StreamValue>> xread(List<String> keys, List<String> startValues)
            throws IllegalStreamItemIdException {
        List<List<StreamValue>> results = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            lru.logKeyAccess(keys.get(i));
            StreamData stream = getOrCreateStreamData(keys.get(i)).getStreamValue();
            StreamId startId = stream.getStreamIdForRead(startValues.get(i));
            results.add(stream.readNextValues(StreamData.MAX_READ_COUNT, startId));
        }
        return results;
    }


    // List operations

    public long lpush(String key, String value) {
        evictIfNeeded(key);
        lru.logKeyAccess(key);
        List<String> list = getOrCreateListData(key).getListValue();
        list.addFirst(value);
        return list.size();
    }

    public long rpush(String key, String value) {
        evictIfNeeded(key);
        lru.logKeyAccess(key);
        List<String> list = getOrCreateListData(key).getListValue();
        list.addLast(value);
        return list.size();
    }

    public String lpop(String key) {
        DataEntry entry = entries.get(key);
        if (entry == null) return null;
        lru.logKeyAccess(key);
        LinkedList<String> list = entry.getListValue();
        if (list.isEmpty()) return null;
        String value = list.removeFirst();
        if (list.isEmpty()) delete(key);
        return value;
    }

    public String rpop(String key) {
        DataEntry entry = entries.get(key);
        if (entry == null) return null;
        lru.logKeyAccess(key);
        LinkedList<String> list = entry.getListValue();
        if (list.isEmpty()) return null;
        String value = list.removeLast();
        if (list.isEmpty()) delete(key);
        return value;
    }

    public List<String> lrange(String key, int start, int end) {
        DataEntry entry = entries.get(key);
        if (entry == null) return List.of();
        lru.logKeyAccess(key);
        List<String> list = entry.getListValue();
        int size = list.size();
        if (size == 0) return List.of();
        if (start < 0) start = size + start;
        if (end < 0) end = size + end;
        start = Math.max(0, start);
        end = Math.min(size - 1, end);
        if (start > end) return List.of();
        return new ArrayList<>(list.subList(start, end + 1));
    }

    // TTL cleanup - scans and removes expired keys
    public void cleanupExpiredKeys() {
        for (String key : entries.keySet()) {
            DataEntry entry = entries.get(key);
            if (entry != null && isExpired(entry)) {
                delete(key);
            }
        }
    }

    // Internal helpers

    Map<String, DataEntry> getEntries() {
        return entries;
    }

    private void evictIfNeeded(String key) {
        if (maxKeys > 0 && !entries.containsKey(key) && lru.size() >= maxKeys) {
            String evictedKey = lru.evictLRUKey();
            if (evictedKey != null) {
                entries.remove(evictedKey);
                log.info("LRU evicted key: {}", evictedKey);
            }
        }
    }

    private DataEntry getOrCreateStreamData(String key) {
        return entries.computeIfAbsent(key,
                k -> new DataEntry(new StreamData(k), clock.millis(), null));
    }

    private DataEntry getOrCreateListData(String key) {
        return entries.computeIfAbsent(key,
                k -> new DataEntry(new LinkedList<String>(), clock.millis(), null));
    }
}