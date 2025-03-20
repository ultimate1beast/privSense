package com.cgi.privsense.dbscanner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.cgi.privsense.dbscanner", "com.cgi.privsense.piidetector"})
public class PrivsenseApplication {

	public static void main(String[] args) {
		SpringApplication.run(PrivsenseApplication.class, args);
	}

}
