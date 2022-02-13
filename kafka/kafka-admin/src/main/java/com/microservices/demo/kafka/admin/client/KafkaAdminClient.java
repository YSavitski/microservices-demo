package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigProperties;
import com.microservices.demo.config.RetryConfigProperties;
import com.microservices.demo.kafka.admin.exception.KafkaClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class KafkaAdminClient {
	private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

	private final KafkaConfigProperties kafkaConfigProperties;
	private final RetryConfigProperties retryConfigProperties;
	private final AdminClient adminClient;
	private final RetryTemplate retryTemplate;
	private final WebClient webClient;

	public KafkaAdminClient(KafkaConfigProperties kafkaConfigProperties,
							RetryConfigProperties retryConfigProperties,
							AdminClient adminClient,
							RetryTemplate retryTemplate,
							WebClient webClient) {
		this.kafkaConfigProperties = kafkaConfigProperties;
		this.retryConfigProperties = retryConfigProperties;
		this.adminClient = adminClient;
		this.retryTemplate = retryTemplate;
		this.webClient = webClient;
	}

	public void createTopics() {
		final List<String> topicNamesToCreate = getTopicNamesToCreate().collect(Collectors.toList());
		CreateTopicsResult createTopicsResult;
		try {
			createTopicsResult = retryTemplate.execute(this::createTopics);
			log.info("Create topic result {}", createTopicsResult.values().values());
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!", t);
		}
		checkTopicsCreated(topicNamesToCreate);
	}

	public void checkSchemaRegistry() {
		int retryCount = 1;
		long checkSleepTimeMs = retryConfigProperties.getSleepDuration().toMillis();
		while(getSchemaRegistryStatus().map(HttpStatus::is2xxSuccessful).orElse(false)) {
			checkMaxRetry(retryCount++, retryConfigProperties.getMaxAttempts());
			sleep(checkSleepTimeMs);
			checkSleepTimeMs *= retryConfigProperties.getMultiplier();
		}
	}

	private Optional<HttpStatus> getSchemaRegistryStatus() {
		try {
			var httpStatus = webClient
					.method(HttpMethod.GET)
					.uri(kafkaConfigProperties.getSchemaRegistryUrl())
					.exchangeToMono(response -> response.bodyToMono(HttpStatus.class))
					.block();
			return Optional.ofNullable(httpStatus);
		} catch (Exception e) {
			log.debug("Error has been occurred during checking schema registry service status {}: {}", kafkaConfigProperties.getSchemaRegistryUrl(), e.getMessage());
			return Optional.of(HttpStatus.SERVICE_UNAVAILABLE);
		}
	}

	private void checkTopicsCreated(final List<String> topicNamesToCreate) {
		Collection<TopicListing> fetchedTopics = getTopics();
		int retryCount = 1;
		long checkSleepTimeMs = retryConfigProperties.getSleepDuration().toMillis();
		for (String topicNameToCreate : topicNamesToCreate) {
			while (!isTopicCreated(fetchedTopics, topicNameToCreate)) {
				checkMaxRetry(retryCount++, retryConfigProperties.getMaxAttempts());
				sleep(checkSleepTimeMs);
				checkSleepTimeMs *= retryConfigProperties.getMultiplier();
				fetchedTopics = getTopics();
			}
		}
	}

	private void sleep(long checkSleepTimeMs) {
		try {
			Thread.sleep(checkSleepTimeMs);
		} catch (InterruptedException e) {
			throw new KafkaClientException("Error while sleeping for waiting new created topics!", e);
		}
	}

	private void checkMaxRetry(int retry, int maxAttempts) {
		if (retry > maxAttempts) throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!");
	}

	private boolean isTopicCreated(Collection<TopicListing> topicNamesToCreate, String topicNameToCreate) {
		if (CollectionUtils.isEmpty(topicNamesToCreate)) return false;
		return topicNamesToCreate.stream().anyMatch(topic -> StringUtils.equals(topic.name(), topicNameToCreate));
	}

	private CreateTopicsResult createTopics(RetryContext retryContext) {
		final List<String> topicNamesToCreate = getTopicNamesToCreate().collect(Collectors.toList());
		log.info("Creating {} topic(s), attempt {}", topicNamesToCreate.size(), retryContext.getRetryCount());
		final List<NewTopic> topicsToCreate = topicNamesToCreate.stream()
				.map(topicName -> new NewTopic(
						topicName,
						kafkaConfigProperties.getPartitionsNumber(),
						kafkaConfigProperties.getReplicationFactor()))
				.collect(Collectors.toList());
		return adminClient.createTopics(topicsToCreate);
	}

	private Collection<TopicListing> getTopics() {
		try {
			return retryTemplate.execute(this::doGetTopics);
		} catch (Throwable t) {
			throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!", t);
		}
	}

	private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
		final String[] topicNamesToCreate = getTopicNamesToCreate().toArray(String[]::new);
		log.info("Reading kafka topic {}, attempt {}", topicNamesToCreate, retryContext.getRetryCount());
		final Collection<TopicListing> topics = adminClient.listTopics().listings().get();
		if (!CollectionUtils.isEmpty(topics)) {
			topics.forEach(topic -> log.debug("Topic with name {}", topic.name()));
		}
		return topics;
	}

	public Stream<String> getTopicNamesToCreate() {
		return kafkaConfigProperties.getTopicNamesToCreate().stream()
				.filter(StringUtils::isNotBlank)
				.map(String::trim);
	}
}
