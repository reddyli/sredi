package com.sredis;

import com.sredis.Server.ServerCentral;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

@SpringBootApplication
public class Application {
	public static void main(String[] args) {
		//Setting up application context
		SpringApplication.run(Application.class, args);
		ServerCentral.exec();
	}
}
