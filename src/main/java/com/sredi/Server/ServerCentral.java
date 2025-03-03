package com.sredi.Server;

import com.sredi.Parser.CommandParser;
import com.sredi.Parser.FileParser;
import com.sredi.Replication.ReplicaHandshake;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.*;

public class ServerCentral {

    public static final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> configStore = new ConcurrentHashMap<>();
    private static final BlockingQueue<String> replicationCommandQueue = new LinkedBlockingQueue<>();
    private static ScheduledExecutorService executorService;
    private static String role = "MASTER";
    private static final String MASTER_REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static int MASTER_REPL_OFFSET = 0;

    public static void exec(String[] args) {
        int port = parseArguments(args);
        loadConfiguration();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            executorService = Executors.newSingleThreadScheduledExecutor();
            ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                clientProcessingPool.submit(() -> {
                    try {
                        process(clientSocket);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static int parseArguments(String[] args) {
        int port = 0;
        List<String> argsList = Arrays.asList(args);
        if (argsList.contains("--port")) {
            port = Integer.parseInt(argsList.get(1));
        }
        if (argsList.contains("--replicaof")) {
            role = "REPLICA";
            port = 6379;
            String masterIP = argsList.get(1);
            configStore.put("masterIP", masterIP);
            String masterPort = argsList.get(2);
            configStore.put("masterPort", masterPort);
            try {
                ReplicaHandshake.replicate(masterIP, Integer.parseInt(masterPort));
            } catch (Exception e) {
                System.out.println("ReplicaHandshake failed: " + e.getMessage());
            }
        }
        return port;
    }

    private static void loadConfiguration() {
        try {
            File file = new File(configStore.getOrDefault("dir", "./"), configStore.getOrDefault("dbfilename", "sredi.rdb"));
            if (file.exists()) {
                FileParser.parseRDBFile(file);
            }
        } catch (Exception e) {
            System.out.println("Error loading configuration: " + e.getMessage());
        }
    }

    private static void process(Socket clientSocket) throws IOException {
        try (InputStream in = clientSocket.getInputStream();
             OutputStreamWriter osw = new OutputStreamWriter(clientSocket.getOutputStream())) {
            List<Object> command;
            while (!(command = CommandParser.parseCommand(in)).isEmpty()) {
                handleCommand(command, osw);
                osw.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            clientSocket.close();
        }
    }

    private static void handleCommand(List<Object> command, OutputStreamWriter osw) throws IOException {
        String cmd = (String) command.get(0);
        switch (cmd.toLowerCase()) {
            case "ping":
                osw.write("+PONG\r\n");
                break;
            case "metadata":
                handleMetadataCommand(osw);
                break;
            case "echo":
                handleEchoCommand(command, osw);
                break;
            case "replconf":
                osw.write("+OK\r\n");
                break;
            case "set":
                handleSetCommand(command, osw);
                break;
            case "get":
                handleGetCommand(command, osw);
                break;
            case "config":
                handleConfigCommand(command, osw);
                break;
            case "keys":
                handleKeysCommand(osw);
                break;
            case "psync":
                handlePsyncCommand(osw);
                break;
            case "info":
                handleInfoCommand(osw);
                break;
            default:
                osw.write("-ERR unknown command\r\n");
        }
    }

    private static void handleMetadataCommand(OutputStreamWriter osw) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("role:").append(role).append("\n");
        sb.append("MASTER_REPL_ID:").append(MASTER_REPL_ID).append("\n");
        sb.append("master_repl_offset:").append(MASTER_REPL_OFFSET).append("\n");
        osw.write(String.format("$%s\r\n%s\r\n", sb.toString().length(), sb));
    }

    private static void handleEchoCommand(List<Object> command, OutputStreamWriter osw) throws IOException {
        osw.write(String.join("\r\n", command.stream().skip(1).toArray(String[]::new)) + "\r\n");
    }

    private static void handleSetCommand(List<Object> command, OutputStreamWriter osw) throws IOException {
        osw.write("+OK\r\n");
        String key = (String) command.get(2);
        StringBuilder respArray = new StringBuilder();
        for (Object s : command) {
            respArray.append((String) s).append("\r\n");
        }
        replicationCommandQueue.add(respArray.toString());
        keyValueStore.put(key, String.join("\r\n", command.stream().skip(3).toArray(String[]::new)) + "\r\n");
        if (command.size() == 9 && ((String) command.get(6)).equalsIgnoreCase("PX")) {
            long delay = Long.parseLong((String) command.get(8));
            executorService.schedule(() -> keyValueStore.remove(key), delay, TimeUnit.MILLISECONDS);
        }
    }

    private static void handleGetCommand(List<Object> command, OutputStreamWriter osw) throws IOException {
        String key = (String) command.get(2);
        String value = keyValueStore.get(key);
        osw.write("+" + value + "\r\n");
    }

    private static void handleConfigCommand(List<Object> command, OutputStreamWriter osw) throws IOException {
        if (((String) command.get(2)).equalsIgnoreCase("GET")) {
            String args = (String) command.get(4);
            if (args.equalsIgnoreCase("DIR")) {
                String dir = configStore.get("dir");
                osw.write("*2\r\n$3\r\ndir\r\n$" + dir.length() + "\r\n" + dir + "\r\n");
            } else if (args.equalsIgnoreCase("DBFILENAME")) {
                String dbfilename = configStore.get("dbfilename");
                osw.write("*2\r\n$3\r\ndbfilename\r\n$" + dbfilename.length() + "\r\n" + dbfilename + "\r\n");
            }
        }
    }

    private static void handleKeysCommand(OutputStreamWriter osw) throws IOException {
        String[] keys = keyValueStore.keySet().toArray(new String[0]);
        responseList(osw, keys);
    }

    private static void handlePsyncCommand(OutputStreamWriter osw) throws IOException {
        osw.write("+FULLRESYNC " + MASTER_REPL_ID + " 0\r\n");
        byte[] contents = HexFormat.of().parseHex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
        osw.write("$" + contents.length + "\r\n");
        try {
            while (true) {
                String element = replicationCommandQueue.take();
                osw.write(element);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void handleInfoCommand(OutputStreamWriter osw) throws IOException {
        List<String> kvps = new ArrayList<>();
        kvps.add("role:" + role);
        if (role.equals("MASTER")) {
            kvps.add("MASTER_REPL_ID:" + MASTER_REPL_ID);
            kvps.add("master_repl_offset:" + MASTER_REPL_OFFSET);
        }
        responseList(osw, kvps.toArray(new String[0]));
    }

    private static void responseList(OutputStreamWriter osw, String... values) throws IOException {
        osw.write("*" + values.length + "\r\n");
        for (String value : values) {
            osw.write("$" + value.length() + "\r\n");
            osw.write(value + "\r\n");
        }
    }
}