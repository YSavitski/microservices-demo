package com.microservices.demo.twittertokafkaservice.runner;

import com.microservices.demo.config.TwitterToKafkaServiceConfigProperties;
import com.microservices.demo.twittertokafkaservice.listener.TwitterKafkaStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {
	private static final Logger log = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfigProperties twitterToKafkaServiceConfigProperties;
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;

	private TwitterStream twitterStream;

	public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigProperties twitterToKafkaServiceConfigProperties, TwitterKafkaStatusListener twitterKafkaStatusListener) {
		this.twitterToKafkaServiceConfigProperties = twitterToKafkaServiceConfigProperties;
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
	}

	@Override
	public void start() throws TwitterException {
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(twitterKafkaStatusListener);
		setTwitterStreamFilter();
	}

	@PreDestroy
	public void shutdown() {
		if (twitterStream != null) {
			log.warn("Closing twitter connection!");
			twitterStream.shutdown();
		}
	}

	private void setTwitterStreamFilter() {
		final String[] filteringKeywords = twitterToKafkaServiceConfigProperties.getTwitterKeywords().toArray(String[]::new);
		var filterQuery = new FilterQuery(filteringKeywords);
		twitterStream.filter(filterQuery);
		log.info("Started filtering twitter stream for keywords {}", Arrays.toString(filteringKeywords));
	}
}
