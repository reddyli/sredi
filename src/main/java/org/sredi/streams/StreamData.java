package org.sredi.streams;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import org.sredi.resp.RespValue;

// Storage for a single stream: ordered list of IDs + map of ID -> values
public class StreamData {
    public static final int MAX_READ_COUNT = 100;

    @Getter
    private final String streamKey;
    private final OrderedArrayList<StreamId> streamIds = new OrderedArrayList<>();
    private final Map<StreamId, RespValue[]> dataValues = new HashMap<>();

    public StreamData(String streamKey) {
        this.streamKey = streamKey;
    }

    // Adds entry with given or auto-generated ID, returns the actual ID used
    public StreamId add(String itemId, Clock clock, RespValue[] values) throws IllegalStreamItemIdException {
        StreamId streamId = parseOrGenerateId(itemId, clock);
        validateNewId(streamId);

        streamIds.add(streamId);
        dataValues.put(streamId, values);
        return streamId;
    }

    // Reads up to 'count' entries after startId
    public List<StreamValue> readNextValues(int count, StreamId startId) {
        count = Math.min(count, MAX_READ_COUNT);
        if (count == 0) return List.of();

        int index = streamIds.findNext(startId);
        if (index == streamIds.size()) return List.of();

        List<StreamValue> result = new ArrayList<>();
        int end = Math.min(index + count, streamIds.size());
        for (int i = index; i < end; i++) {
            StreamId id = streamIds.get(i);
            result.add(new StreamValue(id, dataValues.get(id)));
        }
        return result;
    }

    // Returns entries in range [start, end] inclusive
    public List<StreamValue> queryRange(String start, String end) throws IllegalStreamItemIdException {
        StreamId startId = parseRangeParam(start, true);
        StreamId endId = parseRangeParam(end, false);
        return streamIds.range(startId, endId).stream()
                .map(id -> new StreamValue(id, dataValues.get(id)))
                .toList();
    }

    // Parses ID for XREAD: "$" means last ID, otherwise parse normally
    public StreamId getStreamIdForRead(String id) throws IllegalStreamItemIdException {
        return "$".equals(id) ? streamIds.getLast() : StreamId.parse(id);
    }

    public RespValue[] getData(StreamId id) {
        return dataValues.get(id);
    }

    @Override
    public String toString() {
        return "StreamData[key=" + streamKey + ", size=" + dataValues.size() + "]";
    }

    // --- Private helpers ---

    private StreamId parseOrGenerateId(String itemId, Clock clock) throws IllegalStreamItemIdException {
        if ("*".equals(itemId)) {
            return generateAutoId(clock);
        }

        String[] parts = itemId.split("-");
        if (parts.length != 2) {
            throw new IllegalStreamItemIdException("ERR: bad id format: " + itemId);
        }

        long timeId = parseLong(parts[0], itemId);
        int counter = parseCounterOrGenerate(parts[1], timeId, itemId);
        return new StreamId(timeId, counter);
    }

    private StreamId generateAutoId(Clock clock) {
        long now = clock.millis();
        int counter = (!streamIds.isEmpty() && streamIds.last().timeId() == now)
                ? streamIds.last().counter() + 1 : 0;
        return new StreamId(now, counter);
    }

    private int parseCounterOrGenerate(String counterStr, long timeId, String itemId)
            throws IllegalStreamItemIdException {
        if (!"*".equals(counterStr)) {
            return parseInt(counterStr, itemId);
        }
        // Auto-generate counter
        if (streamIds.isEmpty() || streamIds.last().timeId() != timeId) {
            return (timeId == 0L) ? 1 : 0;  // special case: 0-0 is invalid, so start at 0-1
        }
        return streamIds.last().counter() + 1;
    }

    private void validateNewId(StreamId id) throws IllegalStreamItemIdException {
        if (id.compareTo(StreamId.MIN_ID) <= 0) {
            throw new IllegalStreamItemIdException("ERR The ID specified in XADD must be greater than 0-0");
        }
        if (!streamIds.isEmpty() && id.compareTo(streamIds.last()) <= 0) {
            throw new IllegalStreamItemIdException("ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }
    }

    private StreamId parseRangeParam(String param, boolean isStart) throws IllegalStreamItemIdException {
        if ("-".equals(param)) return StreamId.MIN_ID;
        if ("+".equals(param)) return StreamId.MAX_ID;

        String[] parts = param.split("-");
        long timeId = parts[0].isEmpty() ? 0 : parseLong(parts[0], param);

        if (parts.length == 2 && !"*".equals(parts[1])) {
            return StreamId.of(timeId, parseInt(parts[1], param));
        }
        return StreamId.of(timeId, isStart ? 0 : Integer.MAX_VALUE);
    }

    private long parseLong(String s, String context) throws IllegalStreamItemIdException {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new IllegalStreamItemIdException("ERR: bad id format: " + context);
        }
    }

    private int parseInt(String s, String context) throws IllegalStreamItemIdException {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new IllegalStreamItemIdException("ERR: bad id format: " + context);
        }
    }
}
