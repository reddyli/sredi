package com.sredi.Server;

import com.sredi.Parser.Parser;
import org.springframework.beans.factory.annotation.Value;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ServerCentral {
    private static final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();

    @Value("${sredi.server.port}")
    private int port;

    public static void exec() {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
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
        InputStream in = null;
        try (OutputStreamWriter osw = new OutputStreamWriter(clientSocket.getOutputStream())) {
            in = clientSocket.getInputStream();
            List<Object> command;
            while ((command = Parser.parseCommand(in)) != null) {
                String cmd = (String) command.get(0);
                switch (cmd.toLowerCase()) {
                    case "ping":
                        osw.write("+PONG\r\n");
                        break;
                    case "echo":
                        osw.write(String.join("\r\n", command.stream().skip(1).toArray(
                                String[]::new)) +
                                "\r\n");
                        break;
                    case "set":
                        osw.write("+OK\r\n");
                        String key = (String) command.get(2);
                        keyValueStore.put(
                                key, String.join("\r\n", command.stream().skip(3).toArray(
                                        String[]::new)) +
                                        "\r\n");
                        break;
                    case "get":
                        key = (String) command.get(2);
                        osw.write(keyValueStore.getOrDefault(key, "$-1\r\n"));
                        break;
                    default:
                        return;
                }
                osw.flush();

            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            var error = e.getMessage();
        } finally {
            try {
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
