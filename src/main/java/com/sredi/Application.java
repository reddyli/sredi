package com.sredi;

import java.io.IOException;

import com.sredi.utils.ConnectionThread;
import com.sredi.utils.ConnectionUtil;
import lombok.extern.slf4j.Slf4j;

import static com.sredi.Setup.init;
import static com.sredi.Setup.initReplica;


@Slf4j
public class Application {
	public static void main(String[] args) throws IOException {

		var portNumber = init(args);
		var serverSocket = ConnectionUtil.createServerSocket(portNumber);

		initReplica();

		while (true) {
			var clientSocket = serverSocket.accept();
			var ClientThread = new ConnectionThread(clientSocket);
			ClientThread.start();
		}
	}
}
