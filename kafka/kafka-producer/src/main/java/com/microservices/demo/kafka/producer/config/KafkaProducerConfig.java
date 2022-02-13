package com.microservices.demo.kafka.producer.config;

import com.microservices.demo.config.KafkaConfigProperties;
import com.microservices.demo.config.KafkaProducerConfigProperties;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig<K extends Serializable, V extends SpecificRecordBase> {

	private final KafkaConfigProperties kafkaConfigProperties;
	private final KafkaProducerConfigProperties kafkaProducerConfigProperties;

	public KafkaProducerConfig(KafkaConfigProperties kafkaConfigProperties, KafkaProducerConfigProperties kafkaProducerConfigProperties) {
		this.kafkaConfigProperties = kafkaConfigProperties;
		this.kafkaProducerConfigProperties = kafkaProducerConfigProperties;
	}

	@Bean
	public Map<String, Object> producerConfig() {
		final Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigProperties.getBootstrapServers());
		props.put(kafkaConfigProperties.getSchemaRegistryUrlKey(), kafkaConfigProperties.getSchemaRegistryUrl());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigProperties.getKeySerializerClass());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigProperties.getValueSerializerClass());
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerConfigProperties.getBatchSize() *
				kafkaProducerConfigProperties.getBatchSizeBoostFactor());
		props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerConfigProperties.getLinger().toMillis());
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProducerConfigProperties.getCompressionType());
		props.put(ProducerConfig.ACKS_CONFIG, kafkaProducerConfigProperties.getAcks());
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerConfigProperties.getRequestTimeoutMs());
		props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfigProperties.getRetryCount());
		return props;
	}

	@Bean
	public ProducerFactory<K, V> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}

	@Bean
	public KafkaTemplate<K, V> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory(), true);
	}
}
