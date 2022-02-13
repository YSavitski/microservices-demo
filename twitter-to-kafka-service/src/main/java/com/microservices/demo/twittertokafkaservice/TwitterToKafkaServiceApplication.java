package com.microservices.demo.twittertokafkaservice;

import com.microservices.demo.twittertokafkaservice.init.StreamInitializer;
import com.microservices.demo.twittertokafkaservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

	private final StreamRunner twitterStreamRunner;
	private final StreamInitializer streamInitializer;

	public TwitterToKafkaServiceApplication(StreamRunner twitterStreamRunner, StreamInitializer streamInitializer) {
		this.twitterStreamRunner = twitterStreamRunner;
		this.streamInitializer = streamInitializer;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("App starts...");
		streamInitializer.init();
		twitterStreamRunner.start();
	}
}
