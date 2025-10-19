package jkml;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageRetriever implements Closeable {

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

	private final Logger logger = LoggerFactory.getLogger(MessageRetriever.class);

	private final Consumer<String, String> consumer;

	private final String topic;

	private List<TopicPartition> assignedPartitions = List.of();

	public MessageRetriever(Consumer<String, String> consumer, String topic) {
		this.consumer = consumer;
		this.topic = topic;
	}

	@Override
	public void close() throws IOException {
		this.consumer.close();
	}

	private List<TopicPartition> getPartitions() {
		var partitions = new ArrayList<TopicPartition>();
		for (var info : consumer.partitionsFor(topic)) {
			partitions.add(new TopicPartition(topic, info.partition()));
		}
		return partitions;
	}

	private void assign() {
		if (assignedPartitions.isEmpty()) {
			var partitions = getPartitions();
			logger.info("Assigning all partitions under topic: {}", topic);
			consumer.assign(partitions);
			assignedPartitions = partitions;
		}
	}

	private static Map<TopicPartition, Long> createTimestamps(List<TopicPartition> partitions, Instant timestamp) {
		var timestamps = new HashMap<TopicPartition, Long>();
		var ms = timestamp.toEpochMilli();
		partitions.forEach(p -> timestamps.put(p, ms));
		return timestamps;
	}

	public void seekToBeginning() {
		assign();

		logger.info("Seeking to the first offset for each partition under topic: {}", topic);
		consumer.seekToBeginning(assignedPartitions);
	}

	public void seekToEnd() {
		assign();

		logger.info("Seeking to the last offset for each partition under topic: {}", topic);
		consumer.seekToEnd(assignedPartitions);
	}

	public void seekToTime(Instant timestamp) {
		assign();

		logger.info("Seeking to the earliest offset created on or after {} for each partition under topic: {}", timestamp, topic);
		var timeOffsets = consumer.offsetsForTimes(createTimestamps(assignedPartitions, timestamp));
		var endOffsets = consumer.endOffsets(assignedPartitions);
		var offsets = new HashMap<>(endOffsets);
		for (var tos : timeOffsets.entrySet()) {
			if (tos.getValue() != null) {
				offsets.put(tos.getKey(), tos.getValue().offset());
			}
		}
		offsets.forEach(consumer::seek);
	}

	public ConsumerRecords<String, String> poll() {
		assign();

		logger.info("Fetching messages from topic: {}", topic);
		var messages = consumer.poll(POLL_TIMEOUT);
		logger.info("Fetched message count: {}", messages.count());
		return messages;
	}

}
