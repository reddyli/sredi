package com.sredi.Server;

import com.sredi.Parser.CommandParser;
import com.sredi.Parser.FileParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServerCentral {

    public static final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> configStore = new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService;
    private static String role = "MASTER";

    public static void exec(String[] args) {

        int port = 6379;

        List<String> argsList = Arrays.asList(args);
        if(argsList.contains("--port")) {
            port = Integer.parseInt(argsList.get(1));
        }
        if(argsList.contains("--replicaof")) {
            role = "REPLICA";
        }

        try {
            File file = new File(configStore.getOrDefault("dir", "./"), configStore.getOrDefault("dbfilename","sredi.rdb"));
            if (file.exists()) {
                FileParser.parseRDBFile(file);
            }
        } catch (Exception e) {}


            try (ServerSocket serverSocket = new ServerSocket(port)) {
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



private static void process(Socket clientSocket) throws IOException {
        InputStream in = null;
        try (OutputStreamWriter osw = new OutputStreamWriter(clientSocket.getOutputStream())) {
            in = clientSocket.getInputStream();
            List<Object> command;
            while (!(command = CommandParser.parseCommand(in)).isEmpty()) {
                String cmd = (String) command.get(0);
                switch (cmd.toLowerCase()) {
                    case "ping":
                        osw.write("+PONG\r\n");
                        break;
                    case "metadata":
                        StringBuilder sb = new StringBuilder();
                        sb.append("role:").append(role).append("\n");
                        sb.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n");
                        sb.append("master_repl_offset:0\n");
                        osw.write(String.format("$%s\r\n%s\r\n", sb.toString().length(), sb));
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
                            if(((String)command.get(6)).equalsIgnoreCase("PX")) {
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
                        String value = keyValueStore.get(key);
                        osw.write("+" + value + "\r\n");
                        break;
                    case "config":
                        if(((String)command.get(2)).equalsIgnoreCase("GET")) {
                            String args = (String) command.get(4);
                            if(args.equalsIgnoreCase("DIR")) {
                                String dir = configStore.get("dir");
                                osw.write("*2\r\n$3\r\ndir\r\n$"+dir.length()+ "\r\n" + dir +"\r\n");
                            } else if(args.equalsIgnoreCase("DBFILENAME")) {
                                String dbfilename = configStore.get("dbfilename");
                                osw.write("*2\r\n$3\r\ndbfilename\r\n$"+dbfilename.length()+ "\r\n" + dbfilename +"\r\n");
                            }
                            break;
                        }
                    case "keys":
                        String[] keys = keyValueStore.keySet().toArray(new String[0]);
                        responseList(osw, keys);
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

    private static void responseList(OutputStreamWriter osw, String... values) throws IOException {
        osw.write("*" + values.length + "\r\n");
        for (String value : values) {
            osw.write("$" + value.length() + "\r\n");
            osw.write(value + "\r\n");
        }
    }

}
