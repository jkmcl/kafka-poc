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

public sealed class TopicReader permits ConcurrentTopicReader {

	static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);

	private final Logger logger = LoggerFactory.getLogger(TopicReader.class);

	private final String topic;

	final Supplier<Consumer<String, String>> consumerFactory;

	final List<TopicPartition> partitions = new ArrayList<>();

	public TopicReader(String topic, Supplier<Consumer<String, String>> consumerFactory) {
		this.topic = topic;
		this.consumerFactory = consumerFactory;
	}

	public List<ConsumerRecord<String, String>> read(Instant minTimestamp) {
		var messages = new ArrayList<ConsumerRecord<String, String>>();
		process(minTimestamp, m -> {
			messages.ensureCapacity(messages.size() + m.count());
			m.forEach(messages::add);
		});
		return messages;
	}

	public void process(Instant minTimestamp, java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		logger.info("Fetching messages from topic: {}", topic);

		commitToTime(minTimestamp);

		poll(processor);
	}

	void commitToTime(Instant timestamp) {
		logger.info("Committing fetch offsets to the earliest ones with timestamp >= {} (topic: {})", timestamp, topic);

		try (var consumer = consumerFactory.get()) {
			partitions.clear();
			partitions.addAll(ConsumerUtils.partitionsFor(consumer, topic));

			var endOffsets = consumer.endOffsets(partitions);
			var timeOffsets = ConsumerUtils.offsetsForTime(consumer, partitions, timestamp);
			var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
			endOffsets.forEach((partition, endOffset) -> {
				var timeOffset = timeOffsets.get(partition);
				offsets.put(partition, new OffsetAndMetadata((timeOffset == null) ? endOffset : timeOffset.offset()));
			});

			consumer.assign(partitions);
			consumer.commitSync(offsets);
		}
	}

	void poll(java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		try (var consumer = consumerFactory.get()) {
			consumer.assign(partitions);
			poll(-1, consumer, processor);
		}

	}

	void poll(int index, Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {

		var prefix = index < 0 ? "" : "Consumer[" + index + "]: ";

		if (logger.isDebugEnabled()) {
			logger.debug("{}Fetching messages from partition(s): {}", prefix,
					TopicUtils.join(", ", consumer.assignment()));
		}

		var batchCount = 0;
		var messageCount = 0;
		do {
			var messages = consumer.poll(POLL_TIMEOUT);
			++batchCount;
			var count = messages.count();
			logger.debug("{}Fetched {} message(s)", prefix, count);
			messageCount += count;
			processor.accept(messages);
		} while (ConsumerUtils.hasLag(consumer));

		logger.debug("{}Fetched {} message(s) in {} batch(es)", prefix, messageCount, batchCount);
	}

}
