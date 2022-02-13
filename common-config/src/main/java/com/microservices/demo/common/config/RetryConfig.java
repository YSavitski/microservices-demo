package com.microservices.demo.common.config;

import com.microservices.demo.config.RetryConfigProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {
	private final RetryConfigProperties retryConfigProperties;

	public RetryConfig(RetryConfigProperties retryConfigProperties) {
		this.retryConfigProperties = retryConfigProperties;
	}

	@Bean
	public RetryTemplate retryTemplate() {
		var retryTemplate = new RetryTemplate();

		var exponentialBackOffPolicy = new ExponentialBackOffPolicy();
		exponentialBackOffPolicy.setInitialInterval(retryConfigProperties.getInitialInterval().toMillis());
		exponentialBackOffPolicy.setMaxInterval(retryConfigProperties.getMaxInterval().toMillis());
		exponentialBackOffPolicy.setMultiplier(retryConfigProperties.getMultiplier());
		retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

		var simpleRetryPolicy = new SimpleRetryPolicy();
		simpleRetryPolicy.setMaxAttempts(retryConfigProperties.getMaxAttempts());
		retryTemplate.setRetryPolicy(simpleRetryPolicy);

		return retryTemplate;
	}
}
