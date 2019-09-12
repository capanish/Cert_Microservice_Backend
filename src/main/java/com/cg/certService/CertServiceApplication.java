package com.cg.certService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan("com.cg.certService.*")
public class CertServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CertServiceApplication.class, args);
	}

}
