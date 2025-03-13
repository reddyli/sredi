package com.sredi.utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.sredi.common.SocketOperations;
import com.sredi.rdb.RdbUtil;
import com.sredi.replication.MasterConnectionHolder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Executor {
	private final Socket socket;
	private final OutputStream outputStream;
	private final BufferedWriter writer;
	private final boolean isReplication;

	public boolean parseAndExecute(List<String> inputParams) {
		try {
			var command = Command.parseCommand(inputParams.getFirst());
			if (command == null) {
				returnCommonErrorMessage(null);
				return false;
			}

			var data = executeCommand(inputParams);
			if (data == null || isReplication) {
				return true;
			}
			var outputStr = ResultData.convertToOutputString(data);
			log.debug("output: {}", outputStr);

			if (command.isReadOperation()) {
				SocketOperations.sendStringToSocket(writer, outputStr);
			}
			if (command.isWriteOperation()) {
				MasterConnectionHolder.propagateCommand(inputParams);
			}
			return true;
		} catch (RuntimeException e) {
			log.warn("command execute error - inputParams: {}", inputParams, e);
			returnCommonErrorMessage(null);
			return false;
		} catch (IOException e) {
			log.error("IOException", e);
			return false;
		}
	}

	public void returnCommonErrorMessage(String detailErrorMessage) {
		try {
			if (detailErrorMessage != null) {
				writer.write("-ERR " + detailErrorMessage + "\r\n");
			} else {
				writer.write("-ERR\r\n");
			}
			writer.flush();
		} catch (IOException e) {
			log.error("IOException", e);
		}
	}

	private List<ResultData> executeCommand(List<String> inputParams) {
		var command = Command.parseCommand(inputParams.getFirst());
		var restParams = inputParams.subList(1, inputParams.size());

		return switch (command) {
			case PING -> ping();
			case ECHO -> echo(restParams);
			case GET -> get(restParams);
			case SET -> set(restParams);
			case DEL -> del(restParams);
			case CONFIG -> config(restParams);
			case KEYS -> keys();
			case INFO -> info(restParams);
			case REPLCONF -> replconf(restParams);
			case PSYNC -> psync(restParams);
			case WAIT -> wait(restParams);
		};
	}

	private List<ResultData> ping() {
		return List.of(new ResultData(DataType.SIMPLE_STRINGS, Constant.OUTPUT_PONG));
	}

	private List<ResultData> echo(List<String> args) {
		if (args.size() != 1) {
			throw new ExecuteException("execute error - echo need exact 1 args");
		}

		return ResultData.getBulkStringData(args.getFirst());
	}

	private List<ResultData> get(List<String> args) {
		if (args.size() != 1) {
			throw new ExecuteException("execute error - get need exact 1 args");
		}

		var key = args.getFirst();
		var findResult = Repository.get(key);

		return ResultData.getBulkStringData(findResult);
	}

	private List<ResultData> set(List<String> args) {
		if (args.size() < 2) {
			throw new ExecuteException("execute error - set need more than 2 args");
		}

		var key = args.getFirst();
		var value = args.get(1);
		var expireTime = new AtomicLong(-1L);

		if (args.size() >= 4) {
			if ("px".equalsIgnoreCase(args.get(2))) {
				var milliseconds = Long.parseLong(args.get(3));
				expireTime.set(milliseconds);
			}
		}

		Repository.set(key, value);

		if (expireTime.get() > 0L) {
			Repository.expireWithExpireTime(key, expireTime.get());
		}

		return ResultData.getSimpleResultData(DataType.SIMPLE_STRINGS, Constant.OUTPUT_OK);
	}

	private List<ResultData> del(List<String> args) {
		var key = args.getFirst();

		Repository.del(key);
		return ResultData.getSimpleResultData(DataType.SIMPLE_STRINGS, Constant.OUTPUT_OK);
	}

	private List<ResultData> config(List<String> args) {
		if (args.size() != 2) {
			throw new ExecuteException("execute error - config need exact 2 params");
		}

		if (Constant.COMMAND_PARAM_GET.equalsIgnoreCase(args.getFirst())) {
			var key = args.get(1);
			var value = Repository.configGet(key);
			return ResultData.getArrayData(key, value);
		} else {
			throw new ExecuteException("execute error - not supported option");
		}
	}

	private List<ResultData> keys() {
		var keys = Repository.getKeys();
		return ResultData.getArrayData(keys.toArray(new String[0]));
	}

	private List<ResultData> info(List<String> restParam) {
		if (!restParam.getFirst().equalsIgnoreCase("replication")) {
			return null;
		}

		var result = new StringBuilder("# Replication\n");

		for (var setting : Repository.getAllReplicationSettings()) {
			result.append(setting.getKey());
			result.append(":");
			result.append(setting.getValue());
			result.append("\n");
		}

		return ResultData.getBulkStringData(result.toString());
	}

	private List<ResultData> replconf(List<String> restParam) {
		log.info("REPLCONF INPUT - param: {}", restParam);
		if ("GETACK".equalsIgnoreCase(restParam.getFirst()) && "*".equalsIgnoreCase(restParam.get(1))) {
			try {
				var offset = Repository.getReplicationSetting("a", "0");
				var message = ResultData.getArrayData("REPLCONF", "ACK", offset);

				SocketOperations.sendStringToSocket(writer, ResultData.convertToOutputString(message));
				return null;
			} catch (Exception e) {
				log.error("IOException", e);
			}
		} else if ("ACK".equalsIgnoreCase(restParam.getFirst()) && !isReplication) {
			var offset = Integer.parseInt(restParam.getLast());
			var connectionProvider = MasterConnectionHolder.findProvider(socket);
			connectionProvider.setPresentAck(offset);
			return null;
		}
		if ("listening-port".equalsIgnoreCase(restParam.getFirst()) || "capa".equalsIgnoreCase(restParam.getFirst())) {
			var host = socket.getInetAddress().getHostAddress();
			var connectionPort = socket.getPort();
			var port = Integer.parseInt(restParam.get(1));

			log.info("ipAddress: {}, innerPort: {}, port:{}", host, connectionPort, port);
			return ResultData.getSimpleResultData(DataType.SIMPLE_STRINGS, "OK");
		}
		return null;
	}

	private List<ResultData> psync(List<String> restParam) {
		var replId = Repository.getReplicationSetting("master_replid");
		var offset = Repository.getReplicationSetting("master_repl_offset");

		var result = ResultData.getSimpleResultData(DataType.SIMPLE_STRINGS, "FULLRESYNC %s %s".formatted(replId, offset));
		var message = Base64.getDecoder().decode(RdbUtil.EMPTY_RDB);
		var sizeData = new ResultData(DataType.BULK_STRINGS, String.valueOf(message.length));

		result = Stream.concat(result.stream(), Stream.of(sizeData)).toList();

		try {
			SocketOperations.sendStringToSocket(writer, ResultData.convertToOutputString(result));
			SocketOperations.sendBytesToSocket(outputStream, message);

			var ipAddress = socket.getInetAddress().getHostAddress();
			var innerPort = socket.getPort();

			log.info("ipAddress: {}, innerPort: {}", ipAddress, innerPort);
			MasterConnectionHolder.addConnectedList(socket, writer);
		} catch (Exception e) {
			log.error("IOException", e);
		}

		return result;
	}

	private List<ResultData> wait(List<String> args) {
		if (args.size() != 2) {
			throw new ExecuteException("execute error - wait need exact 2 params");
		}

		var needReplica = Integer.parseInt(args.getFirst());
		var timeLimit = Integer.parseInt(args.getLast());

		return ResultData.getSimpleResultData(DataType.INTEGER, String.valueOf(MasterConnectionHolder.getFullySyncedReplicaCount(needReplica, timeLimit)));
	}
}
