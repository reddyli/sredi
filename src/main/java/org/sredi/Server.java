package org.sredi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.setup.SetupOptions;
import org.sredi.storage.CentralRepository;

import java.io.IOException;
import java.time.Clock;

public class Server implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    SetupOptions options;
    CentralRepository repository = null;

    public Server(String... args) {
        options = new SetupOptions();
        if (!options.parseArgs(args)) {
            throw new RuntimeException("Invalid arguments");
        }
    }

    public void run() {
        repository = CentralRepository.newInstance(options, Clock.systemUTC());
        try {
            repository.start();
            repository.runCommandLoop();
            log.info("Event loop terminated");

        } catch (IOException e) {
            log.error("IOException: {}", e.getMessage());
        } catch (InterruptedException e) {
            log.error("InterruptedException: {}", e.getMessage());
        } finally {
            repository.shutdown();
            repository = null;
        }
    }

}
