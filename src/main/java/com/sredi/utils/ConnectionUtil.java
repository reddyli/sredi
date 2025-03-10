package com.sredi.utils;

import java.io.IOException;
import java.net.ServerSocket;

public class ConnectionUtil {
	private ConnectionUtil() {}

	public static ServerSocket createServerSocket(int port) {
		try {
			var serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				try {
					serverSocket.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}));

			return serverSocket;
		} catch (IOException e) {
			throw new RuntimeException(String.format("IOException: %s%n", e.getMessage()));
		}
	}
}
