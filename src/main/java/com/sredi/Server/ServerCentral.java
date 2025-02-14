package com.sredi.Server;

import com.sredi.Parser.Parser;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServerCentral {

    private static final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService;

    public static void exec() {
        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            executorService = Executors.newSingleThreadScheduledExecutor();

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
        InputStream in = null;
        try (OutputStreamWriter osw = new OutputStreamWriter(clientSocket.getOutputStream())) {
            in = clientSocket.getInputStream();
            List<Object> command;
            while (!(command = Parser.parseCommand(in)).isEmpty()) {
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
                        if(command.size() == 9) {
                            String expiry;
                            if((expiry = (String)command.get(6)).equalsIgnoreCase("PX")) {
                                long delay = Long.parseLong((String)command.get(8));
                                String finalCommand = (String) command.get(2);
                                executorService.schedule(
                                        () -> keyValueStore.remove(finalCommand), delay, TimeUnit.MILLISECONDS
                                );
                            }
                        }
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
