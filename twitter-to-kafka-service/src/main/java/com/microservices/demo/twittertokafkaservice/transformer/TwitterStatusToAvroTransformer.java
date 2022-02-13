package com.microservices.demo.twittertokafkaservice.transformer;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Service;
import twitter4j.Status;

@Service
public class TwitterStatusToAvroTransformer {
	public TwitterAvroModel getTwitterAvroModelFromStatus(final Status status) {
		return TwitterAvroModel.newBuilder()
				.setId(status.getId())
				.setUserId(status.getUser().getId())
				.setText(status.getText())
				.setCreatedAt(status.getCreatedAt().getTime())
				.build();
	}
}
