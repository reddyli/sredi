package org.sredi.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

// ArrayList with binary search for sorted elements (callers must add in order)
public class OrderedArrayList<T extends Comparable<T>> extends ArrayList<T> {

    // Returns index of first element > val, or size() if none
    public int findNext(T val) {
        if (isEmpty() || val.compareTo(get(0)) < 0) {
            return 0;
        }
        if (val.compareTo(last()) >= 0) {
            return size();
        }
        return binarySearchLeftmost(0, size(), i -> get(i).compareTo(val) > 0);
    }

    public T last() {
        return isEmpty() ? null : get(size() - 1);
    }

    // Returns sublist of elements in [startId, endId] inclusive
    public List<T> range(T startId, T endId) {
        int start = binarySearchLeftmost(0, size(), i -> get(i).compareTo(startId) >= 0);
        int end = findNext(endId);
        return subList(start, end);
    }

    // Binary search for leftmost index where condition is true
    private int binarySearchLeftmost(int left, int right, IntPredicate condition) {
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (condition.test(mid)) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }
}
