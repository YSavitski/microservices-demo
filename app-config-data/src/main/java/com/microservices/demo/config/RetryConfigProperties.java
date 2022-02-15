package com.microservices.demo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Data
@Configuration
@ConfigurationProperties(prefix = "retry-config")
public class RetryConfigProperties {
	private Duration initialInterval;
	private Duration maxInterval;
	private Double multiplier;
	private Integer maxAttempts;
	private Duration sleepDuration;
}
