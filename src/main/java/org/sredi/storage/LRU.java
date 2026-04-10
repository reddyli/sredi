package org.sredi.storage;

import java.util.HashMap;
import java.util.Map;


// Can also use LinkedHashMap from collections
public class LRU {

    static class Node {
        String key;
        Node prev;
        Node next;
    }

    private final Node head;
    private final Node tail;
    private final Map<String, Node> map;

    public LRU() {
        head = new Node();
        tail = new Node();
        head.next = tail;
        tail.prev = head;
        map = new HashMap<>();
    }

    public void logKeyAccess(String key) {
        if (!map.containsKey(key)) {
            Node node = new Node();
            node.key = key;
            addBeforeTail(node);
            map.put(key, node);
        } else {
            Node node = map.get(key);
            unlink(node);
            addBeforeTail(node);
        }
    }

    public String evictLRUKey() {
        if (head.next == tail) return null;
        Node lruNode = head.next;
        unlink(lruNode);
        map.remove(lruNode.key);
        return lruNode.key;
    }

    public void remove(String key) {
        Node node = map.get(key);
        if (node == null) return;
        unlink(node);
        map.remove(key);
    }

    public int size() {
        return map.size();
    }

    private void unlink(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void addBeforeTail(Node node) {
        node.prev = tail.prev;
        node.next = tail;
        tail.prev.next = node;
        tail.prev = node;
    }
}
