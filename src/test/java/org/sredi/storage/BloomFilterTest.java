package org.sredi.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

class BloomFilterTest {

    @Test
    void rejectsInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(0, 0.01));
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(-1, 0.01));
        assertThrows(IllegalArgumentException.class,
                () -> new BloomFilter((1L << 30) + 1, 0.01));
    }

    @Test
    void rejectsInvalidErrorRate() {
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(100, 0.0));
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(100, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(100, -0.5));
        assertThrows(IllegalArgumentException.class, () -> new BloomFilter(100, 1.5));
    }

    @Test
    void sizingMatchesFormula() {
        BloomFilter bf = new BloomFilter(1000, 0.01);
        // m = ceil(-1000 * ln(0.01) / ln(2)^2) = 9586, k = round(9586/1000 * ln(2)) = 7
        assertEquals(9586, bf.getM());
        assertEquals(7, bf.getK());
        assertEquals(1000, bf.getCapacity());
        assertEquals(0.01, bf.getErrorRate());
    }

    @Test
    void addedItemIsContained() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        assertTrue(bf.add("apple".getBytes(UTF_8)));
        assertTrue(bf.mightContain("apple".getBytes(UTF_8)));
    }

    @Test
    void missingItemIsNotContained() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        assertFalse(bf.mightContain("apple".getBytes(UTF_8)));
    }

    @Test
    void addReturnsTrueOnceFalseAfter() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        byte[] item = "apple".getBytes(UTF_8);
        assertTrue(bf.add(item));
        assertFalse(bf.add(item));
        assertEquals(1, bf.getItemCount());
    }

    @Test
    void differentItemsHashDifferently() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        bf.add("apple".getBytes(UTF_8));
        assertFalse(bf.mightContain("appel".getBytes(UTF_8)));
        assertFalse(bf.mightContain("banana".getBytes(UTF_8)));
    }

    @Test
    void emptyByteArrayIsHandled() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        assertTrue(bf.add(new byte[0]));
        assertTrue(bf.mightContain(new byte[0]));
    }

    @Test
    void nullThrows() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        assertThrows(NullPointerException.class, () -> bf.add(null));
        assertThrows(NullPointerException.class, () -> bf.mightContain(null));
    }

    @Test
    void falsePositiveRateUnderTarget() {
        int n = 1000;
        double p = 0.01;
        BloomFilter bf = new BloomFilter(n, p);

        Random rng = new Random(42);
        Set<String> inserted = new HashSet<>(n);
        while (inserted.size() < n) {
            inserted.add("in-" + rng.nextLong());
        }
        for (String s : inserted) {
            bf.add(s.getBytes(UTF_8));
        }

        int probes = 10_000;
        int falsePositives = 0;
        for (int i = 0; i < probes; i++) {
            String candidate = "out-" + rng.nextLong();
            if (inserted.contains(candidate)) continue;
            if (bf.mightContain(candidate.getBytes(UTF_8))) {
                falsePositives++;
            }
        }

        double observed = (double) falsePositives / probes;
        assertTrue(observed < 0.03,
                "False positive rate too high: " + observed + " (target " + p + ")");
    }
}
