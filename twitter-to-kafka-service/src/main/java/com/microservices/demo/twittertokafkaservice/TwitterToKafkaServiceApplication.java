package com.microservices.demo.twittertokafkaservice;

import com.microservices.demo.twittertokafkaservice.config.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

	public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData) {
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
	}

	public static void main(String[] args) {
		SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		log.info("App starts...");
		log.info(Arrays.toString(twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[] {})));
		log.info(twitterToKafkaServiceConfigData.getWelcomeMessage());
	}
}
