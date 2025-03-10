package com.sredi.replication;

import java.io.BufferedWriter;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MasterConnectionHolder {
	private static final Map<ReplicationConnectionInfo, Integer> WAITING_LIST = new HashMap<>();
	private static final List<MasterConnectionProvider> MASTER_CONNECTION_PROVIDERS = new ArrayList<>();
	private static boolean isCallOperation = false;

	public static void propagateCommand(List<String> inputParams) {
		log.info("propagate start! inputParams: {}", inputParams);
		isCallOperation = true;

		MASTER_CONNECTION_PROVIDERS.forEach(provider -> {
			provider.sendMessage(inputParams);
		});
	}

	public static void createNewWaitingConnection(String ipAddress, int connectionPort, int slavePort) {
		WAITING_LIST.put(new ReplicationConnectionInfo(ipAddress, connectionPort), slavePort);
	}

	public static MasterConnectionProvider findProvider(Socket socket) {
		return MASTER_CONNECTION_PROVIDERS.stream()
			.filter(masterConnectionProvider -> masterConnectionProvider.getSocket().getPort() == socket.getPort())
			.findFirst()
			.orElse(null);
	}

	public static void addConnectedList(Socket socket, BufferedWriter writer) {
		MASTER_CONNECTION_PROVIDERS.add(new MasterConnectionProvider(socket, writer));
	}

	public static int getFullySyncedReplicaCount(int desireCount, long limitTime) {
		var start = Instant.now();
		var result = 0;

		if (!isCallOperation) {
			return MASTER_CONNECTION_PROVIDERS.size();
		}

		MASTER_CONNECTION_PROVIDERS.forEach(masterConnectionProvider -> {
			log.info("desiredAck: {}, actualAck: {}", masterConnectionProvider.getDesiredAck(), masterConnectionProvider.getPresentAck());
			masterConnectionProvider.sendAck();
		});

		while (Duration.between(start, Instant.now()).toMillis() < limitTime) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {

			}

			result = (int)MASTER_CONNECTION_PROVIDERS.stream()
				.filter(MasterConnectionProvider::isFullySynced)
				.count();

			if (result >= desireCount) {
				return result;
			}
		}

		return result;
	}
}
