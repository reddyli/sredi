package com.sredi.replication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.sredi.common.SocketOperations;
import com.sredi.utils.Command;
import com.sredi.utils.ResultData;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CommandExecutor {

	public static String sendCommand(BufferedReader reader, BufferedWriter writer, Command command, List<String> args) {
		var inputParams = new ArrayList<String>();
		inputParams.add(command.name());
		inputParams.addAll(args);

		try {
			var sendMessage = ResultData.getArrayData(inputParams.toArray(new String[0]));
			SocketOperations.sendStringToSocket(writer, ResultData.convertToOutputString(sendMessage));

			var result = reader.readLine();

			log.info("send command complete - command: {}, args: {}, result: {}", command, args, result);
			return result;
		} catch (IOException e) {
			log.error("IOException", e);
			return null;
		}
	}
}
