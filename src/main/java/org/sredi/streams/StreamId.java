package org.sredi.streams;

// Stream entry ID: "timestamp-sequence" (e.g., "1234567890-0"), ordered by timestamp then sequence
public record StreamId(long timeId, int counter) implements Comparable<StreamId> {

    public static final StreamId MIN_ID = new StreamId(0L, 0);
    public static final StreamId MAX_ID = new StreamId(Long.MAX_VALUE, Integer.MAX_VALUE);

    public String getId() {
        return timeId + "-" + counter;
    }

    @Override
    public int compareTo(StreamId other) {
        int cmp = Long.compare(this.timeId, other.timeId);
        return cmp != 0 ? cmp : Integer.compare(this.counter, other.counter);
    }

    @Override
    public String toString() {
        return getId();
    }

    public static StreamId of(long timeId, int counter) {
        return new StreamId(timeId, counter);
    }

    public static StreamId parse(String s) throws IllegalStreamItemIdException {
        try {
            if (s.contains("-")) {
                String[] parts = s.split("-");
                return of(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
            }
            return of(Long.parseLong(s), 0);
        } catch (NumberFormatException e) {
            throw new IllegalStreamItemIdException("Invalid stream id: " + s);
        }
    }
}