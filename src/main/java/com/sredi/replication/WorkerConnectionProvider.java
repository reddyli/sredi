package com.sredi.replication;

import com.sredi.common.Pair;
import com.sredi.common.SocketOperations;
import com.sredi.utils.Command;
import com.sredi.utils.Executor;
import com.sredi.store.Repository;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.List;
import java.util.Objects;


@Slf4j
public class WorkerConnectionProvider {
	private Socket clientSocket;
	private OutputStream outputStream;
	private BufferedReader reader;
	private BufferedWriter writer;

	public void init(String masterHost, int masterPort) {
		try {
			this.clientSocket = new Socket(masterHost, masterPort);
			this.outputStream = clientSocket.getOutputStream();
			this.reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			this.writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
			handShake();
			receiveMessage();
		} catch (Exception e) {

		}
	}

	public void handShake() {
		ping();
		replconf();
		psync();
	}

	private void ping() {
		CommandExecutor.sendCommand(reader, writer, Command.PING, List.of());
	}

	private void replconf() {
		CommandExecutor.sendCommand(reader, writer, Command.REPLCONF, List.of("listening-port", Repository.configGet("port")));
		CommandExecutor.sendCommand(reader, writer, Command.REPLCONF, List.of("capa", "psync2"));
	}

	private void psync() {
		var replicationId = Objects.requireNonNullElse(Repository.getReplicationSetting("master_replid"), "?");
		var replicationOffset = Objects.requireNonNullElse(Repository.getReplicationSetting("master_repl_offset"), "-1");

		CommandExecutor.sendCommand(reader, writer, Command.PSYNC, List.of(replicationId, replicationOffset));
		receiveRdb();
	}

	private void receiveRdb() {
		try {
			var lengthOfFile = reader.readLine();
			var length = Integer.valueOf(lengthOfFile.substring(1));
			var rdbInput = new StringBuilder();

			for (int i = 1; i < length; i++) {
				rdbInput.append((char)(reader.read()));
			}

			log.info("rdb file received. length:{}, input:{}", lengthOfFile, rdbInput);
		} catch (IOException e) {
			log.error("IOException", e);
		}
	}

	private void receiveMessage() {
		new Thread(() -> {
			var srediExecutor = new Executor(clientSocket, outputStream, writer, true);
			Pair<Integer, List<String>> inputInfo;
			List<String> inputParams;

			while ((inputInfo = SocketOperations.parseSocketInputToCommand(reader)) != null) {
				inputParams = inputInfo.second();
				if (inputParams.isEmpty()) {
					continue;
				}
				log.debug("inputParams: {}", inputParams);

				var result = srediExecutor.parseAndExecute(inputParams);
				if (result) {
					var offset = Integer.parseInt(Repository.getReplicationSetting("a", "0"));
					log.info("Offset renewed! - offset: {}", offset + inputInfo.first());
					Repository.setReplicationSetting("a", String.valueOf(offset + inputInfo.first()));
				}
			}
		}).start();
	}
}
