package com.sredi.rdb.model;

import java.util.List;

public record RdbDbInfo(
	int dbNumber,
	int hashTableSize,
	int expireHashTableSize,
	List<RdbExpirePair> rdbExpirePairs,
	List<RdbPair> rdbPairs
) {
}