package com.sredi.Parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {

    public static List<Object> parseCommand(InputStream inputStream) throws IOException {

        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));

        List<Object> ret = new ArrayList<>();
        try {
            String line1 = in.readLine();
            if (line1 == null) {
                ret.add("exit");
                return ret;
            }
            if (line1.charAt(0) != '*') {
                throw new RuntimeException("ERR command must be an array ");
            }
            int nEle = Integer.parseInt(line1.substring(1));
            in.readLine();
            ret.add(in.readLine());
            // read data
            String line = null;
            for (int i = 1; i < nEle && (line = in.readLine()) != null; i++) {
                if (line.isEmpty())
                    continue;
                char type = line.charAt(0);
                switch (type) {
                    case '$':
                        System.out.println("parse bulk string: " + line);
                        ret.add(line);
                        ret.add(in.readLine());
                        break;
                    case ':':
                        System.out.println("parse int: " + line);
                        ret.add(String.valueOf(type));
                        ret.add(Integer.parseInt(line.substring(1)));
                        break;
                    default:
                        System.out.println("default: " + line);
                        break;
                }
            }
        } catch (IOException e) {
            System.out.println("Parse failed " + e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.out.println("Parse failed " + e.getMessage());
        }
        System.out.println("Command: " +
                String.join(" ", ret.toArray(String[]::new)));
        return ret;
    }
}
