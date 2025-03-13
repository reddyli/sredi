package com.sredi.store;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum KeyType {
    STRING,
    LIST,
    SET,
    HASH,
    ZSET,
    STREAM,
    NONE;
}
