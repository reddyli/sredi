package com.sredis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {

		//Setting up application context
		SpringApplication.run(Application.class, args);

		System.out.println("Logs from your program will appear here!");

		ServerSocket serverSocket = null;
		Socket clientSocket = null;
		int port = 6379;
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			clientSocket = serverSocket.accept();
			System.out.println("Client connected!");

			DataInputStream dataInputStream = new DataInputStream(clientSocket.getInputStream());
			DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
			dataOutputStream.writeBytes("+PONG\r\n");

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

}
