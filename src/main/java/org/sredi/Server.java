package org.sredi;

import org.sredi.setup.SetupOptions;

import java.io.IOException;
import java.time.Clock;

public class Server implements Runnable {
    SetupOptions options;
    CentralRepository repository = null;

    public Server(String... args) {
        options = new SetupOptions();
        if (!options.parseArgs(args)) {
            throw new RuntimeException("Invalid arguments");
        }
    }

    public Server(SetupOptions options) {
        this.options = options;
    }

    public void terminate() {
        if (repository != null) {
            repository.terminate();
        }
    }

    public void run() {
        repository = CentralRepository.newInstance(options, Clock.systemUTC());
        try {
            repository.start();
            repository.runCommandLoop();
            System.out.println(String.format("Event loop terminated"));

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("InterruptedException: " + e.getMessage());
        } finally {
            repository.shutdown();
            repository = null;
        }
    }

}
