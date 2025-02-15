package com.sredi;

import com.sredi.Server.ServerCentral;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
	public static void main(String[] args) {
		//Setting up application context
		SpringApplication.run(Application.class, args);
		ServerCentral.exec(args);
	}
}
