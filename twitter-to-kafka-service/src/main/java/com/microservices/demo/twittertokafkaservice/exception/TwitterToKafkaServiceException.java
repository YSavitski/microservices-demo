package com.microservices.demo.twittertokafkaservice.exception;

public class TwitterToKafkaServiceException extends RuntimeException {
	public TwitterToKafkaServiceException() {
	}

	public TwitterToKafkaServiceException(String message) {
		super(message);
	}

	public TwitterToKafkaServiceException(String message, Throwable cause) {
		super(message, cause);
	}
}
