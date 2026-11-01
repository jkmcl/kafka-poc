package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicReader {

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);

	private final Logger logger = LoggerFactory.getLogger(TopicReader.class);

	private final String topic;

	private final Supplier<Consumer<String, String>> consumerFactory;

	private final List<TopicPartition> partitions = new ArrayList<>();

	public TopicReader(String topic, Supplier<Consumer<String, String>> consumerFactory) {
		this.topic = topic;
		this.consumerFactory = consumerFactory;
	}

	private void fetchPartitions() {
		// Partition info is fetched only once by design
		if (partitions.isEmpty()) {
			try (var consumer = consumerFactory.get()) {
				partitions.addAll(TopicUtils.toPartitions(consumer.partitionsFor(topic)));
			}
		}
	}

	public void commitToTime(Instant timestamp) {
		logger.info("Committing fetch offsets to the earliest ones with timestamp >= {} (topic: {})", timestamp, topic);

		fetchPartitions();

		try (var consumer = consumerFactory.get()) {
			consumer.assign(partitions);
			var endOffsets = consumer.endOffsets(partitions);
			var timeOffsets = consumer.offsetsForTimes(TopicUtils.createTimestamps(partitions, timestamp));
			var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
			endOffsets.forEach((partition, endOffset) -> {
				var timeOffset = timeOffsets.get(partition);
				offsets.put(partition, new OffsetAndMetadata((timeOffset == null) ? endOffset : timeOffset.offset()));
			});
			consumer.commitSync(offsets);
			consumer.unsubscribe();
		}
	}

	public List<ConsumerRecord<String, String>> poll() {
		logger.info("Fetching messages from topic: {}", topic);

		var messages = new ArrayList<ConsumerRecord<String, String>>();

		fetchPartitions();

		try (var consumer = consumerFactory.get()) {
			consumer.assign(partitions);

			poll(consumer, m -> {
				messages.ensureCapacity(messages.size() + m.count());
				m.forEach(messages::add);
			});
		}

		logger.info("Fetched message count: {}", messages.size());
		return messages;
	}

	void poll(Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		if (logger.isInfoEnabled()) {
			logger.info("Fetching messages from partition(s): {}", TopicUtils.join(", ", consumer.assignment()));
		}

		var batchCount = 0;
		var messageCount = 0;
		while (true) {
			var messages = consumer.poll(POLL_TIMEOUT);
			var count = messages.count();
			logger.debug("Fetched {} message(s)", count);
			if (count == 0) {
				break;
			}
			++batchCount;
			messageCount += count;
			processor.accept(messages);
		}

		logger.debug("Fetched {} message(s) in {} batch(es)", messageCount, batchCount);
	}

}
