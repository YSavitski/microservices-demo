package com.microservices.demo.twittertokafkaservice;

import com.microservices.demo.config.TwitterToKafkaServiceConfiguration;
import com.microservices.demo.twittertokafkaservice.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.demo")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

	private final TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration;
	private final StreamRunner twitterStreamRunner;

	public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration, StreamRunner twitterKafkaStreamRunner) {
		this.twitterToKafkaServiceConfiguration = twitterToKafkaServiceConfiguration;
		this.twitterStreamRunner = twitterKafkaStreamRunner;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("App starts...");
		log.info(Arrays.toString(twitterToKafkaServiceConfiguration.getTwitterKeywords().toArray(String[]::new)));
		log.info(twitterToKafkaServiceConfiguration.getWelcomeMessage());
		twitterStreamRunner.start();
	}
}
