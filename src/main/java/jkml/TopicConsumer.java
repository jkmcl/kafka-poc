package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicConsumer {

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);

	private final Logger logger = LoggerFactory.getLogger(TopicConsumer.class);

	private final String topic;

	private final Supplier<Consumer<String, String>> consumerFactory;

	private final List<TopicPartition> partitions = new ArrayList<>();

	public TopicConsumer(String topic, Supplier<Consumer<String, String>> consumerFactory) {
		this.topic = topic;
		this.consumerFactory = consumerFactory;
	}

	private void getPartitions() {
		if (partitions.isEmpty()) {
			try (var localConsumer = consumerFactory.get()) {
				partitions.addAll(TopicUtils.toPartitions(localConsumer.partitionsFor(topic)));
			}
		}
	}

	public void commitToTime(Instant timestamp) {
		logger.info("Committing fetch offsets to the earliest ones with timestamp >= {} (topic: {})", timestamp, topic);

		getPartitions();

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

	private List<Consumer<String, String>> createConsumers() {
		logger.info("Creating and assigning one consumer for each partition in topic: {}", topic);

		var partitionCount = partitions.size();
		var consumers = new ArrayList<Consumer<String, String>>(partitionCount);
		for (var i = 0; i < partitionCount; ++i) {
			consumers.add(consumerFactory.get());
		}

		for (var i = 0; i < partitionCount; ++i) {
			consumers.get(i).assign(List.of(partitions.get(i)));
		}

		return consumers;
	}

	public List<ConsumerRecord<String, String>> poll() {
		logger.info("Fetching messages from topic: {}", topic);

		getPartitions();

		var consumers = createConsumers();
		var messages = new ArrayList<ConsumerRecord<String, String>>();
		try {
			var index = 0;
			for (var consumer : consumers) {
				var msgs = poll(index++, consumer);
				messages.ensureCapacity(messages.size() + msgs.size());
				messages.addAll(msgs);
			}
		} finally {
			var index = 0;
			for (var consumer : consumers) {
				try {
					consumer.close();
				} catch (Exception e) {
					logger.warn("Failed to close Consumer[{}]", index, e);
				}
			}
		}

		logger.info("Fetched message count: {} (topic: {})", messages.size(), topic);
		return messages;
	}

	List<ConsumerRecord<String, String>> poll(int index, Consumer<String, String> consumer) {
		if (logger.isInfoEnabled()) {
			logger.info("Consumer[{}] fetching messages from partitions: {}", index,
					TopicUtils.join(", ", consumer.assignment()));
		}
		var allMessages = new ArrayList<ConsumerRecord<String, String>>();
		while (true) {
			var messages = consumer.poll(POLL_TIMEOUT);
			var count = messages.count();
			logger.info("Consumer[{}] fetched message count: {}", index, count);
			if (count == 0) {
				break;
			}

			allMessages.ensureCapacity(allMessages.size() + messages.count());
			for (var cr : messages) {
				allMessages.add(cr);
			}
		}
		logger.info("Consumer[{}] total fetched message count: {}", index, allMessages.size());
		return allMessages;
	}

}
