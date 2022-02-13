package com.microservices.demo.twittertokafkaservice.listener;

import com.microservices.demo.config.KafkaConfigProperties;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import com.microservices.demo.twittertokafkaservice.transformer.TwitterStatusToAvroTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
	private static final Logger log = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

	private final KafkaConfigProperties kafkaConfigProperties;
	private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
	private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

	public TwitterKafkaStatusListener(KafkaConfigProperties kafkaConfigProperties, KafkaProducer<Long, TwitterAvroModel> kafkaProducer, TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
		this.kafkaConfigProperties = kafkaConfigProperties;
		this.kafkaProducer = kafkaProducer;
		this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
	}

	@Override
	public void onStatus(Status status) {
		log.info("Received status text {} sending to kafka topic {}", status.getText(), kafkaConfigProperties.getTopicName());
		var twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
		kafkaProducer.send(kafkaConfigProperties.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
	}
}
