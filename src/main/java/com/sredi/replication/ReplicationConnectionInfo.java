package com.sredi.replication;

public record ReplicationConnectionInfo(
	String ipAddress,
	int connectionPort
) {
}
