package org.sredi.storage;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StripedLock {
    
    private static final int NUM_STRIPES = 16;
    private final ReentrantReadWriteLock[] locks;

    public StripedLock() {
        this.locks = new ReentrantReadWriteLock[NUM_STRIPES];
        for (int i = 0; i < NUM_STRIPES; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }
    
    private int bucket(String key) {
        return Math.abs(key.hashCode() % NUM_STRIPES);
    }

    public void readLock(String key) {
        locks[bucket(key)].readLock().lock();
    }

    public void readUnlock(String key) {
        locks[bucket(key)].readLock().unlock();
    }

    public void writeLock(String key) {
        locks[bucket(key)].writeLock().lock();
    }

    public void writeUnlock(String key) {
        locks[bucket(key)].writeLock().unlock();
    }
}
