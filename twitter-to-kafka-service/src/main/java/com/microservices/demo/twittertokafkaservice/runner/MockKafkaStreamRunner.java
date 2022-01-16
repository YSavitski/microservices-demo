package com.microservices.demo.twittertokafkaservice.runner;

import com.microservices.demo.config.TwitterToKafkaServiceConfiguration;
import com.microservices.demo.twittertokafkaservice.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twittertokafkaservice.listener.TwitterKafkaStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {
	private static final Logger log = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration;
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;

	public MockKafkaStreamRunner(TwitterToKafkaServiceConfiguration twitterToKafkaServiceConfiguration, TwitterKafkaStatusListener twitterKafkaStatusListener) {
		this.twitterToKafkaServiceConfiguration = twitterToKafkaServiceConfiguration;
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
	}

	private static final SecureRandom RANDOM = new SecureRandom();
	private static final String[] WORDS = new String[]{
			"potenti",
			"nullam",
			"ac",
			"tortor",
			"vitae",
			"purus",
			"faucibus",
			"ornare",
			"suspendisse",
			"sed",
			"nisi",
			"lacus",
			"sed",
			"viverra",
			"tellus",
			"in",
			"hac",
			"habitasse",
			"platea",
			"dictumst",
			"vestibulum",
			"rhoncus",
			"est",
			"pellentesque",
			"elit"
	};
	private static final String TWEET_AS_ROW_JSON =
		"{" +
			"\"created_at\":\"{0}\"," +
			"\"id\":\"{1}\"," +
			"\"text\":\"{2}\"," +
			"\"user\":{\"id\":\"{3}\"}" +
		"}";
	private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

	@Override
	public void start() throws TwitterException {
		final String[] filteringKeywords = twitterToKafkaServiceConfiguration.getTwitterKeywords().toArray(new String[0]);
		final int minTweetLength = twitterToKafkaServiceConfiguration.getMockMinTweetLength();
		final int maxTweetLength = twitterToKafkaServiceConfiguration.getMockMaxTweetLength();
		log.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(filteringKeywords));
		simulateTwitterStream(filteringKeywords, minTweetLength, maxTweetLength);
	}

	private void simulateTwitterStream(String[] filteringKeywords, int minTweetLength, int maxTweetLength) {
		Executors.newSingleThreadExecutor().submit(() -> {
			try {
				while (true) {
					String formattedTweetAsRawJson = getFormattedTweet(filteringKeywords, minTweetLength, maxTweetLength);
					Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
					twitterKafkaStatusListener.onStatus(status);
					sleep(twitterToKafkaServiceConfiguration.getMockSleep());
				}
			} catch (TwitterException e) {
				log.error("Error creating twitter status!", e);
			}
		});



	}

	private void sleep(Duration mockSleep) {
		try {
			Thread.sleep(mockSleep.toMillis());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!");
		}
	}

	private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
		final String[] params = new String[]{
				ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
				getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
		};

		return FormatTweetAsJsonWithParams(params);
	}

	private String FormatTweetAsJsonWithParams(String[] params) {
		var tweet = TWEET_AS_ROW_JSON;
		for (int i = 0; i< params.length; i++) {
			tweet = tweet.replace("{"+i+"}", params[i]);
		}
		return tweet;
	}

	private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
		var tweetBuilder = new StringJoiner(" ");
		int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
		return constructRandomTweet(keywords, tweetBuilder, tweetLength);
	}

	private String constructRandomTweet(String[] keywords, StringJoiner tweetBuilder, int tweetLength) {
		for (int i = 0; i < tweetLength; i++) {
			tweetBuilder.add(WORDS[RANDOM.nextInt(WORDS.length)]);
			if (i == tweetLength /2) {
				tweetBuilder.add(keywords[RANDOM.nextInt(keywords.length)]);
			}
		}
		return tweetBuilder.toString().trim();
	}
}
