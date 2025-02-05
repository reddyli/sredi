package com.sredis.Server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerCentral {

    public static void exec() {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)){
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try {
                        process(clientSocket);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }


    private static void process(Socket clientSocket) {
        OutputStream out = null;
        BufferedReader in = null;
        try {
            out = clientSocket.getOutputStream();
            in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                if (line.toUpperCase().contains("PING")) {
                    out.write("+PONG\r\n".getBytes());
                    out.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
