package com.sredi.rdb.model;

import com.sredi.utils.Repository;

import java.util.List;


public record Rdb(
	List<AuxField> auxFields,
	List<RdbDbInfo> rdbDbInfos
) {
	public void init() {
		for (var dbInfo : rdbDbInfos) {
			dbInfo.rdbPairs().forEach(rdbPair -> Repository.set(rdbPair.key(), rdbPair.value()));
			dbInfo.rdbExpirePairs().forEach(rdbExpirePair -> Repository.setWithExpireTimestamp(rdbExpirePair.key(), rdbExpirePair.value(), rdbExpirePair.expireTime()));
		}
	}
}
