package com.microservices.demo.twittertokafkaservice.init;

import com.microservices.demo.config.KafkaConfigProperties;
import com.microservices.demo.kafka.admin.client.KafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamInitializer implements StreamInitializer {

	private static final Logger log = LoggerFactory.getLogger(KafkaStreamInitializer.class);

	private final KafkaConfigProperties kafkaConfigProperties;
	private final KafkaAdminClient kafkaAdminClient;

	public KafkaStreamInitializer(KafkaConfigProperties kafkaConfigProperties, KafkaAdminClient kafkaAdminClient) {
		this.kafkaConfigProperties = kafkaConfigProperties;
		this.kafkaAdminClient = kafkaAdminClient;
	}

	@Override
	public void init() {
		kafkaAdminClient.createTopics();
		kafkaAdminClient.checkSchemaRegistry();
		log.info("Topics with name {} are ready for operation!", kafkaAdminClient.getTopicNamesToCreate().toArray(String[]::new));
	}
}
