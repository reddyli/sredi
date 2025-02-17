package com.sredi.Server;

import com.sredi.Parser.Parser;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ServerCentral {

    private static final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> configStore = new ConcurrentHashMap<>();
    private static ScheduledExecutorService executorService;

    public static void exec(String[] args) {

        if(args.length > 0) {
            configStore.put("dir", args[0]);
            configStore.put("dbfilename", args[1]);
        }


        // Session RDB File
        try {
        File file = new File(configStore.get("dir"), configStore.get("dbfilename"));
        if (file.exists()) {
            InputStream in = new FileInputStream(file);
            int b;
            int lengthEncoding;
            int valueType;
            byte[] bytes;
            String key;
            String value;
            // skip
            while ((b = in.read()) != -1) {
                // do nothing
                if (b == 0xFB) {
                    getLength(in);
                    getLength(in);
                    break;
                }
            }
            System.out.println("skipped");
            valueType = in.read(); // value-type
            System.out.println("valueType=" + valueType);
            // Key
            lengthEncoding = getLength(in);
            bytes = in.readNBytes(lengthEncoding);
            key = new String(bytes);
            // Value
            lengthEncoding = getLength(in);
            bytes = in.readNBytes(lengthEncoding);
            value = new String(bytes);
            ServerCentral.keyValueStore.put(key, value);
        }
    } catch (Exception e) {
            System.out.println("error");
        }


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

    private static int getLength(InputStream in) throws IOException {
        int length = 0;
        byte b = (byte)in.read();
        switch (b & 0b11000000) {
            case 0 -> {
                length = b & 0b00111111;
            }
            case 128 -> {
                ByteBuffer buffer = ByteBuffer.allocate(2);
                buffer.put((byte) (b & 00111111));
                buffer.put((byte) in.read());
                buffer.rewind();
                length = buffer.getShort();
            }
            case 256 -> {
                ByteBuffer buffer = ByteBuffer.allocate(4);
                buffer.put(b);
                buffer.put(in.readNBytes(3));
                buffer.rewind();
                length = buffer.getInt();
            }
            case 384 -> {
                System.out.println("Special format");
            }
        }
        return length;
    }


private static void process(Socket clientSocket) throws IOException {
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
