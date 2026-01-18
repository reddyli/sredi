package org.sredi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.CentralRepository;

import java.io.IOException;
import java.time.Clock;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final SetupOptions options;

    public Server(String... args) {
        this.options = new SetupOptions();
        if (!options.parseArgs(args)) {
            throw new IllegalArgumentException("Invalid arguments");
        }
    }

    public void run() {
        CentralRepository repository = CentralRepository.newInstance(options, Clock.systemUTC());
        try {
            repository.start();
            repository.runCommandLoop();
            log.info("Event loop terminated");
        } catch (IOException e) {
            log.error("IOException: {}", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("InterruptedException: {}", e.getMessage());
        } finally {
            repository.shutdown();
        }
    }
}
