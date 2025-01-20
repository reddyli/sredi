import java.io.*;
import java.net.*;

public class RedisClient {
    public static void main(String args[]) throws IOException 
    {
        Socket socket = new Socket("localhost", 6379);
        
        // Setup output stream to send data to the server
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        
        // Setup input stream to receive data from the server
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Send message to the server
        out.println("PING");

        // Receive response from the server
        String response = in.readLine();
        System.out.println("Server says: " + response);

        // Close the socket
        socket.close();
    }
}
