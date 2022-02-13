package com.microservices.demo.kafka.producer.service;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

	private static final Logger log = LoggerFactory.getLogger(TwitterKafkaProducer.class);

	private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

	public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public void send(String topicName, Long key, TwitterAvroModel message) {
		log.debug("Sending message='{}' to topic='{}'", message, topicName);
		final ProducerRecord<Long, TwitterAvroModel> producerRecord = new ProducerRecord<>(topicName, key, message);
		kafkaTemplate.send(producerRecord).addCallback(new TwitterProducerCallback(topicName, message));
	}

	private static class TwitterProducerCallback implements ListenableFutureCallback<SendResult<Long, TwitterAvroModel>> {
		private final String topicName;
		private final TwitterAvroModel message;

		public TwitterProducerCallback(String topicName, TwitterAvroModel message) {
			this.topicName = topicName;
			this.message = message;
		}

		@Override
		public void onFailure(Throwable throwable) {
			log.error("Error while sending message {} to the topic {}", message.toString(), topicName, throwable);
		}

		@Override
		public void onSuccess(SendResult<Long, TwitterAvroModel> result) {
			var metadata = result.getRecordMetadata();
			log.debug("Received new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
					metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), System.nanoTime());
		}
	}

	@PreDestroy
	public void close() {
		if (kafkaTemplate != null) {
			log.info("Closing kafka producer!");
			kafkaTemplate.destroy();
		}
	}
}
