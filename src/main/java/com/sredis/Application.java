package com.sredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {

		//Setting up application context
		SpringApplication.run(Application.class, args);
		ServerSocket serverSocket = null;
		Socket clientSocket = null;
		int port = 6379;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);

			while (true) {
				clientSocket = serverSocket.accept();

				process(clientSocket);
			}




		} catch (IOException e) {
			System.out.println("IOException: " + e.getMessage());
		} finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException e) {
				System.out.println("IOException: " + e.getMessage());
			}
		}
	}

	private static void process(Socket clientSocket) {
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(clientSocket.getInputStream()));
			 BufferedWriter writer = new BufferedWriter(
					 new OutputStreamWriter(clientSocket.getOutputStream()));) {
			String content;
			while ((content = reader.readLine()) != null) {
				System.out.println("::" + content);
				if ("ping".equalsIgnoreCase(content)) {
					writer.write("+PONG\r\n");
					writer.flush();
				} else if ("eof".equalsIgnoreCase(content)) {
					System.out.println("eof");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
