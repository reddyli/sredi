package org.sredi.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LRUTest {

    private LRU lru;

    @BeforeEach
    void setUp() {
        lru = new LRU();
    }

    @Test
    void evictReturnsNullWhenEmpty() {
        assertNull(lru.evictLRUKey());
    }

    @Test
    void evictsLeastRecentlyUsedKey() {
        lru.logKeyAccess("a");
        lru.logKeyAccess("b");
        lru.logKeyAccess("c");

        assertEquals("a", lru.evictLRUKey());
        assertEquals("b", lru.evictLRUKey());
        assertEquals("c", lru.evictLRUKey());
        assertNull(lru.evictLRUKey());
    }

    @Test
    void accessMovesKeyToMostRecent() {
        lru.logKeyAccess("a");
        lru.logKeyAccess("b");
        lru.logKeyAccess("c");
        lru.logKeyAccess("a"); // a moves to most recent

        assertEquals("b", lru.evictLRUKey());
        assertEquals("c", lru.evictLRUKey());
        assertEquals("a", lru.evictLRUKey());
    }

    @Test
    void removeDeletesKey() {
        lru.logKeyAccess("a");
        lru.logKeyAccess("b");
        lru.logKeyAccess("c");

        lru.remove("b");

        assertEquals(2, lru.size());
        assertEquals("a", lru.evictLRUKey());
        assertEquals("c", lru.evictLRUKey());
    }

    @Test
    void removeNonExistentKeyDoesNothing() {
        lru.logKeyAccess("a");
        lru.remove("z");
        assertEquals(1, lru.size());
    }

    @Test
    void sizeTracksCorrectly() {
        assertEquals(0, lru.size());
        lru.logKeyAccess("a");
        assertEquals(1, lru.size());
        lru.logKeyAccess("b");
        assertEquals(2, lru.size());
        lru.logKeyAccess("a"); // duplicate, no size change
        assertEquals(2, lru.size());
        lru.evictLRUKey();
        assertEquals(1, lru.size());
    }
}

