package org.sredi.storage;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Probabilistic set membership data structure backed by a packed bit array.
 * Not thread-safe; callers must serialize concurrent writes (DataStore relies on
 * the striped write lock for BF.ADD / BF.RESERVE).
 */
public final class BloomFilter {

    private static final long MAX_CAPACITY = 1L << 30;

    private final long[] bits;
    private final int totalBits;
    private final int totalHashFunctions;

    private final long capacity;
    private final double falsePositiveErrorRate;
    private long itemCount;

    public BloomFilter(long capacity, double falsePositiveErrorRate) {
        if (capacity <= 0 || capacity > MAX_CAPACITY) {
            throw new IllegalArgumentException("Invalid capacity: " + capacity);
        }
        if (falsePositiveErrorRate <= 0.0 || falsePositiveErrorRate >= 1.0) {
            throw new IllegalArgumentException(
                    "FalsePositiveErrorRate must be greater than 0 and less than 1.");
        }
        this.capacity = capacity;
        this.falsePositiveErrorRate = falsePositiveErrorRate;
        this.totalBits = optimalM(capacity, falsePositiveErrorRate);
        this.totalHashFunctions = optimalK(totalBits, capacity);
        this.bits = new long[(totalBits + 63) >>> 6];
    }

    public boolean add(byte[] item) {
        if (item == null) throw new NullPointerException("item");
        int[] indices = indicesFor(item);
        boolean wasNew = false;
        for (int idx : indices) {
            if (!getBit(idx)) {
                wasNew = true;
                setBit(idx);
            }
        }
        if (wasNew) itemCount++;
        return wasNew;
    }

    public boolean mightContain(byte[] item) {
        if (item == null) throw new NullPointerException("item");
        int[] indices = indicesFor(item);
        for (int idx : indices) {
            if (!getBit(idx)) return false;
        }
        return true;
    }

    public long getItemCount() { return itemCount; }
    public int getM() { return totalBits; }
    public int getK() { return totalHashFunctions; }
    public long getCapacity() { return capacity; }
    public double getErrorRate() { return falsePositiveErrorRate; }

    private int[] indicesFor(byte[] item) {
        byte[] digest = md5(item);
        long h1 = ByteBuffer.wrap(digest, 0, 8).getLong();
        long h2 = ByteBuffer.wrap(digest, 8, 8).getLong();
        int[] indices = new int[totalHashFunctions];
        for (int i = 0; i < totalHashFunctions; i++) {
            long combined = h1 + (long) i * h2;
            indices[i] = (int) Math.floorMod(combined, (long) totalBits);
        }
        return indices;
    }

    private void setBit(int index) {
        bits[index >>> 6] |= (1L << (index & 63));
    }

    private boolean getBit(int index) {
        return (bits[index >>> 6] & (1L << (index & 63))) != 0L;
    }

    private static byte[] md5(byte[] item) {
        try {
            return MessageDigest.getInstance("MD5").digest(item);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    private static int optimalM(long n, double p) {
        double mDouble = Math.ceil(-(double) n * Math.log(p) / (Math.log(2) * Math.log(2)));
        if (mDouble > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Required bit array size exceeds Integer.MAX_VALUE: " + mDouble);
        }
        return Math.max(1, (int) mDouble);
    }

    private static int optimalK(int m, long n) {
        int k = (int) Math.round((double) m / (double) n * Math.log(2));
        return Math.max(1, k);
    }
}
