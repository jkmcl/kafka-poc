package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Component
public class MessageRetriever {

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private final Logger logger = LoggerFactory.getLogger(MessageRetriever.class);

	private final ConsumerFactory<String, String> consumerFactory;

	public MessageRetriever(ConsumerFactory<String, String> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	private List<TopicPartition> getPartitions(Consumer<String, String> consumer, String topic) {
		var partitions = new ArrayList<TopicPartition>();
		for (var info : consumer.partitionsFor(topic, TIMEOUT)) {
			partitions.add(new TopicPartition(topic, info.partition()));
		}
		return partitions;
	}

	public Iterable<ConsumerRecord<String, String>> poll(String topic) {
		logger.info("Fetching messages from topic: {}", topic);
		try (var consumer = consumerFactory.createConsumer()) {
			consumer.assign(getPartitions(consumer, topic));
			return consumer.poll(TIMEOUT).records(topic);
		}
	}

	public Iterable<ConsumerRecord<String, String>> pollFromBeginning(String topic) {
		logger.info("Fetching messages from beginning from topic: {}", topic);
		try (var consumer = consumerFactory.createConsumer()) {
			var partitions = getPartitions(consumer, topic);
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			return consumer.poll(TIMEOUT).records(topic);
		}
	}

	private static Map<TopicPartition, Long> createTimestamps(List<TopicPartition> partitions, Instant timestamp) {
		var map = new HashMap<TopicPartition, Long>();
		var ms = Long.valueOf(timestamp.toEpochMilli());
		partitions.forEach(p -> map.put(p, ms));
		return map;
	}

	public Iterable<ConsumerRecord<String, String>> pollFromTimestamp(String topic, Instant timestamp) {
		logger.info("Fetching messages from {} from topic: {}", timestamp, topic);
		try (var consumer = consumerFactory.createConsumer()) {
			var partitions = getPartitions(consumer, topic);
			var partitionsWithMessage = new ArrayList<TopicPartition>();
			var offsets = consumer.offsetsForTimes(createTimestamps(partitions, timestamp), TIMEOUT);
			for (var entry : offsets.entrySet()) {
				if (entry.getValue() != null) {
					partitionsWithMessage.add(entry.getKey());
				}
			}
			if (partitionsWithMessage.isEmpty()) {
				return List.of();
			}
			consumer.assign(partitionsWithMessage);
			for (var part : partitionsWithMessage) {
				consumer.seek(part, offsets.get(part).offset());
			}
			return consumer.poll(TIMEOUT).records(topic);
		}
	}

}
