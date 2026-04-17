package org.sredi.setup;

import lombok.Getter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.constants.ReplicationConstants;

import java.nio.file.Files;
import java.nio.file.Path;

@Getter
public class SetupOptions {
    private static final Logger log = LoggerFactory.getLogger(SetupOptions.class);
    private static final int MAX_PORT = 65535;

    private int port = ReplicationConstants.DEFAULT_PORT;
    private String role = ReplicationConstants.MASTER;
    private String replicaof;
    private int replicaofPort = ReplicationConstants.DEFAULT_PORT;
    private String dir = ".";
    private String dbfilename;
    private transient String password;
    private int maxKeys = -1; // -1 means no limit
    private int maxClients = 100;
    private int maxRps = -1; // -1 means no limit
    private boolean parallel = false;
    private int parallelThreads = Runtime.getRuntime().availableProcessors();

    public boolean parseArgs(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder()
                .longOpt("port")
                .hasArg(true)
                .desc("The port number to use")
                .build());

        options.addOption(Option.builder()
                .longOpt("replicaof")
                .numberOfArgs(2)
                .desc("The host and port of the replica")
                .build());

        options.addOption(Option.builder()
                .longOpt("dir")
                .hasArg(true)
                .desc("The directory where RDB files are stored")
                .build());

        options.addOption(Option.builder()
                .longOpt("dbfilename")
                .hasArg(true)
                .desc("The name of the RDB file")
                .build());

        options.addOption(Option.builder()
                .longOpt("requirepass")
                .hasArg(true)
                .desc("Password")
                .build());

        options.addOption(Option.builder()
                .longOpt("maxkeys")
                .hasArg(true)
                .desc("Maximum number of keys before LRU eviction")
                .build());

        options.addOption(Option.builder()
                .longOpt("maxclients")
                .hasArg(true)
                .desc("Maximum number of concurrent client connections")
                .build());

        options.addOption(Option.builder()
                .longOpt("maxrps")
                .hasArg(true)
                .desc("Maximum requests per second per client")
                .build());

        options.addOption(Option.builder()
                .longOpt("parallel")
                .hasArg(false)
                .desc("Enable parallel command execution with striped locks")
                .build());

        options.addOption(Option.builder()
                .longOpt("parallel-threads")
                .hasArg(true)
                .desc("Number of parallel worker threads")
                .build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("port")) {
                port = Integer.parseInt(cmd.getOptionValue("port"));
                validatePort(port, "port");
                log.info("Port specified: {}", port);
            } else {
                log.info("No port specified, using default.");
            }

            if (cmd.hasOption("replicaof")) {
                String[] replicaofValues = cmd.getOptionValues("replicaof");
                replicaof = replicaofValues[0];
                replicaofPort = Integer.parseInt(replicaofValues[1]);
                validatePort(replicaofPort, "replicaof port");
                role = ReplicationConstants.REPLICA;
                log.info("Replicaof specified: {} {}", replicaof, replicaofPort);
            }

            if (cmd.hasOption("dir")) {
                dir = cmd.getOptionValue("dir");
                if (!Files.isDirectory(Path.of(dir))) {
                    throw new ParseException("Invalid directory: " + dir);
                }
                log.info("Dir specified: {}", dir);
            }

            if (cmd.hasOption("dbfilename")) {
                dbfilename = cmd.getOptionValue("dbfilename");
                log.info("Dbfilename specified: {}", dbfilename);
            }

            if(cmd.hasOption("requirepass")) {
                password = cmd.getOptionValue("requirepass");
                log.info("Authentication Enabled");
            }

            if(cmd.hasOption("maxkeys")) {
                maxKeys = Integer.parseInt(cmd.getOptionValue("maxkeys"));
                log.info("Max keys specified: {}", maxKeys);
            }

            if(cmd.hasOption("maxclients")) {
                maxClients = Integer.parseInt(cmd.getOptionValue("maxclients"));
                log.info("Max clients specified: {}", maxClients);
            }

            if(cmd.hasOption("maxrps")) {
                maxRps = Integer.parseInt(cmd.getOptionValue("maxrps"));
                log.info("Max requests per second specified: {}", maxRps);
            }

            if(cmd.hasOption("parallel")) {
                parallel = true;
                log.info("Parallel execution enabled");
            }

            if(cmd.hasOption("parallel-threads")) {
                parallelThreads = Integer.parseInt(cmd.getOptionValue("parallel-threads"));
                log.info("Parallel threads specified: {}", parallelThreads);
            }

        } catch (ParseException e) {
            log.error("Parsing failed. Reason: {}", e.getMessage());
            new HelpFormatter().printHelp("sredi", options);
            return false;
        }
        return true;
    }

    private void validatePort(int port, String name) throws ParseException {
        if (port <= 0 || port > MAX_PORT) {
            throw new ParseException(name + " must be between 1 and " + MAX_PORT + ": " + port);
        }
    }

    public String getConfigValue(String config) {
        return switch (config) {
            case "port" -> String.valueOf(port);
            case "role" -> role;
            case "replicaof" -> replicaof != null ? replicaof + " " + replicaofPort : null;
            case "dir" -> dir;
            case "dbfilename" -> dbfilename;
            default -> null;
        };
    }
}
