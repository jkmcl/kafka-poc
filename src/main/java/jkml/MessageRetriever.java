package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageRetriever {

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

	private final Logger logger = LoggerFactory.getLogger(MessageRetriever.class);

	private final Consumer<String, String> consumer;

	private final String topic;

	private List<TopicPartition> assignedPartitions = List.of();

	public MessageRetriever(Consumer<String, String> consumer, String topic) {
		this.consumer = consumer;
		this.topic = topic;
	}

	private void assign() {
		if (assignedPartitions.isEmpty()) {
			var partitions = TopicUtils.toPartitions(consumer.partitionsFor(topic));
			logger.info("Assigning all partitions in topic: {}", topic);
			consumer.assign(partitions);
			assignedPartitions = partitions;
		}
	}

	public void commitToTime(Instant timestamp) {
		assign();

		logger.info("Committing fetch offsets to the earliest ones with timestamp >= {}", timestamp);
		var endOffsets = consumer.endOffsets(assignedPartitions);
		var timeOffsets = consumer.offsetsForTimes(TopicUtils.createTimestamps(assignedPartitions, timestamp));
		var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
		endOffsets.forEach((partition, endOffset) -> {
			var timeOffset = timeOffsets.get(partition);
			offsets.put(partition, new OffsetAndMetadata((timeOffset == null) ? endOffset : timeOffset.offset()));
		});
		consumer.commitSync(offsets);

		// Re-assign to effect the change
		consumer.unsubscribe();
		consumer.assign(assignedPartitions);
	}

	public ConsumerRecords<String, String> poll() {
		assign();

		logger.info("Fetching messages");
		var messages = consumer.poll(POLL_TIMEOUT);

		logger.info("Fetched message count: {}", messages.count());
		return messages;
	}

}
