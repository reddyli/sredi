package com.sredi.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Repository {
	private static final ConcurrentHashMap<String, String> KEY_VALUE_STORE = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Long> SREDI_EXPIRY_MAP = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> SREDI_CONFIG_MAP = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, String> SREDI_REPLICATION_INFO_MAP = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, List<String>> SREDI_REPLICATION_CONFIG_MAP = new ConcurrentHashMap<>();

	public static String get(String key) {
		if (SREDI_EXPIRY_MAP.containsKey(key)) {
			var expireTimestamp = SREDI_EXPIRY_MAP.get(key);

			if (Instant.ofEpochMilli(expireTimestamp).isBefore(Instant.now())) {
				SREDI_EXPIRY_MAP.remove(key);
				KEY_VALUE_STORE.remove(key);
				return null;
			}
		}

		return KEY_VALUE_STORE.getOrDefault(key, null);
	}

	public static List<String> getKeys() {
		return KEY_VALUE_STORE.keySet().stream().toList();
	}

	public static String configGet(String key) {
		return SREDI_CONFIG_MAP.getOrDefault(key, null);
	}

	public static void set(String key, String value) {
		KEY_VALUE_STORE.put(key, value);
	}

	public static void del(String key) {
		KEY_VALUE_STORE.remove(key);
	}

	public static void setWithExpireTimestamp(String key, String value, long timeStamp) {
		KEY_VALUE_STORE.put(key, value);
		SREDI_EXPIRY_MAP.put(key, timeStamp);
	}

	public static void configSet(String key, String value) {
		SREDI_CONFIG_MAP.put(key, value);
	}

	public static void expireWithExpireTime(String key, long expireTime) {
		var instant = Instant.now();
		var expireTimeStamp = instant.plus(expireTime, ChronoUnit.MILLIS);

		SREDI_EXPIRY_MAP.put(key, expireTimeStamp.toEpochMilli());
	}

	public static void expire(String key) {
		KEY_VALUE_STORE.remove(key);
	}

	public static void setReplicationSetting(String key, String value) {
		SREDI_REPLICATION_INFO_MAP.put(key, value);
	}

	public static String getReplicationSetting(String key) {
		return SREDI_REPLICATION_INFO_MAP.getOrDefault(key, null);
	}

	public static String getReplicationSetting(String key, String defaultValue) {
		return SREDI_REPLICATION_INFO_MAP.getOrDefault(key, defaultValue);
	}

	public static List<Map.Entry<String, String>> getAllReplicationSettings() {
		return SREDI_REPLICATION_INFO_MAP.keySet().stream()
			.map(key -> Map.entry(key, SREDI_REPLICATION_INFO_MAP.get(key)))
			.toList();
	}

	public static void setReplicationConfig(String key, List<String> values) {
		SREDI_REPLICATION_CONFIG_MAP.put(key, values);
	}

	public static List<String> getReplicationConfig(String key) {
		return SREDI_REPLICATION_CONFIG_MAP.getOrDefault(key, null);
	}
}
