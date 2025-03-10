package com.sredi.replication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;

import com.sredi.common.SocketOperations;
import com.sredi.utils.ResultData;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class MasterConnectionProvider {
	private Socket socket;
	private BufferedReader reader;
	private BufferedWriter writer;
	private boolean isAckRequested = false;
	private boolean isConfirmed = true;
	private int desiredAck = 0;
	private int presentAck = 0;
	public static final int REPLCONF_ACK_BYTE_SIZE = 37;

	public MasterConnectionProvider(Socket socket, BufferedWriter writer) {
		try {
			this.socket = socket;
			this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			this.writer = writer;
		} catch (IOException e) {

		}
	}

	public void sendMessage(List<String> param) {
		try {
			var arrayData = ResultData.getArrayData(param.toArray(new String[0]));
			var message = ResultData.convertToOutputString(arrayData);

			SocketOperations.sendStringToSocket(writer, message);
			desiredAck += message.length();
		} catch (IOException e) {
			log.error("IOException", e);
		}
	}

	public void sendAck() {
		try {
			var ackRequest = ResultData.getArrayData("REPLCONF", "GETACK", "*");
			var message = ResultData.convertToOutputString(ackRequest);

			SocketOperations.sendStringToSocket(writer, message);
			desiredAck += message.length();
			isAckRequested = true;
		} catch (IOException e) {
			log.error("IOException", e);
		}
	}

	public boolean isFullySynced() {
		if (desiredAck - REPLCONF_ACK_BYTE_SIZE <= presentAck) {
			isConfirmed = true;
		}

		return desiredAck - REPLCONF_ACK_BYTE_SIZE <= presentAck;
	}
}
