package com.sredi.Replication;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ReplicaHandshake {

    public static void replicate(String hostname, int masterPortNumber)
            throws IOException {
        try {
            Socket replicaSocket =
                    new Socket(hostname, masterPortNumber);
            OutputStream output = replicaSocket.getOutputStream();
            InputStream input = replicaSocket.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String handShakeMsg = "*1\r\n$4\r\nping\r\n";
            output.write(handShakeMsg.getBytes(StandardCharsets.UTF_8));
            System.out.println(in.readLine());
            /* Plan after PING
            * 1. 2 x REPLCONF
            * For now receive a dummy RDB as sync response.
            * */
            output.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes(StandardCharsets.UTF_8));
            System.out.println(in.readLine());

            output.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes(StandardCharsets.UTF_8));
            System.out.println("Handshake sent");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
