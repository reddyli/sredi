package com.sredi.Replication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ReplicaHandshake {

    public static void replicate(String hostname, int masterPortNumber)
            throws IOException {
        try {
            Socket replicaSocket =
                    new Socket(hostname, masterPortNumber);
            OutputStream output = replicaSocket.getOutputStream();
            String handShakeMsg = "*1\r\n$4\r\nping\r\n";
            output.write(handShakeMsg.getBytes(StandardCharsets.UTF_8));
            output.flush();
            /* Plan after PING
            * 1. 2 x REPLCONF
            * 2. 1 x PSYNC ID + OFFSET
            * */
            System.out.println("Handshake sent");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
