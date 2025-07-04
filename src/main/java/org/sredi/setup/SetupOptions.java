package org.sredi.setup;

import lombok.Getter;
import org.apache.commons.cli.*;
import org.sredi.constants.ReplicationConstants;

import java.io.File;

@Getter
public class SetupOptions {
    private int port = ReplicationConstants.DEFAULT_PORT;
    private String role = ReplicationConstants.MASTER;
    private String replicaof = null;
    private int replicaofPort = ReplicationConstants.DEFAULT_PORT;
    private String dir = ".";
    private String dbfilename = null;

    public boolean parseArgs(String[] args) {
        // Apache Commons CLI Util
        Options options = new Options();

        Option portOption = Option.builder()
                .longOpt("port")
                .hasArg(true)
                .desc("The port number to use")
                .required(false).type(Number.class)
                .build();

        Option replicaofOption = Option.builder()
                .longOpt("replicaof")
                .numberOfArgs(2)
                .desc("The host and port of the replica")
                .required(false)
                .build();

        Option dirOption = Option.builder()
                .longOpt("dir")
                .hasArg(true)
                .desc("The directory where RDB files are stored")
                .required(false)
                .build();

        Option dbfilenameOption = Option.builder()
                .longOpt("dbfilename")
                .hasArg(true)
                .valueSeparator(' ')
                .desc("The name of the RDB file")
                .required(false)
                .build();

        options.addOption(portOption);
        options.addOption(replicaofOption);
        options.addOption(dirOption);
        options.addOption(dbfilenameOption);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            // Check if the port option was provided
            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
                System.out.println("Port specified: " + getPort());
                if (port <= 0 || port > 65535) {
                    throw new ParseException("Port must be less than or equal to 65535: " + port);
                }
            } else {
                System.out.println("No port specified, using default.");
            }

            if (cmd.hasOption("replicaof")) {
                String[] replicaofStrings = cmd.getOptionValues("replicaof");
                replicaof = replicaofStrings[0];
                replicaofPort = Integer.parseInt(replicaofStrings[1]);
                System.out.println("Replicaof specified: " + getReplicaof() + " " + getReplicaofPort());
                if (replicaofPort <= 0 || replicaofPort > 65535) {
                    throw new ParseException("Port must be less than or equal to 65535: " + replicaofPort);
                }

                role = ReplicationConstants.REPLICA;
            }

            if (cmd.hasOption("dir")) {
                dir = cmd.getOptionValue("dir");
                System.out.println("Dir specified: " + getDir());
                // check if dir is a valid directory
                File dirFile = new File(getDir());
                if (!dirFile.isDirectory()) {
                    throw new ParseException("Invalid directory: " + getDir());
                }
            }

            if (cmd.hasOption("dbfilename")) {
                dbfilename = cmd.getOptionValue("dbfilename");
                System.out.println("Dbfilename specified: " + getDbfilename());
            }

        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SetupOptions", options);
            return false;
        }
        return true;
    }

    public String getConfigValue(String config) {
        return switch (config) {
            case "port" -> String.valueOf(port);
            case "role" -> role;
            case "replicaof" -> replicaof + " " + replicaofPort;
            case "dir" -> dir;
            case "dbfilename" -> dbfilename;
            default -> null;
        };
    }

}
