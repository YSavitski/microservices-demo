package com.microservices.demo.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfiguration {
	private List<String> twitterKeywords;
	private String welcomeMessage;
	private Boolean enableMockTweets;
	private Duration mockSleep;
	private Integer mockMinTweetLength;
	private Integer mockMaxTweetLength;
}
