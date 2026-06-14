package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentTopicReader {

	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(3);

	private final Logger logger = LoggerFactory.getLogger(ConcurrentTopicReader.class);

	private final String topic;

	private final Supplier<Consumer<String, String>> consumerFactory;

	private final List<TopicPartition> partitions = new ArrayList<>();

	public ConcurrentTopicReader(String topic, Supplier<Consumer<String, String>> consumerFactory) {
		this.topic = topic;
		this.consumerFactory = consumerFactory;
	}

	private void getPartitions() {
		if (partitions.isEmpty()) {
			try (var consumer = consumerFactory.get()) {
				partitions.addAll(TopicUtils.toPartitions(consumer.partitionsFor(topic)));
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

	private void destroyConsumers(List<Consumer<String, String>> consumers) {
		var index = 0;
		for (var consumer : consumers) {
			try {
				consumer.close();
				++index;
			} catch (Exception e) {
				logger.warn("Failed to close Consumer[{}]", index, e);
			}
		}
	}

	public List<ConsumerRecord<String, String>> poll() {
		logger.info("Fetching messages from topic: {}", topic);

		getPartitions();

		var consumers = createConsumers();

		var execSvc = Executors.newFixedThreadPool(partitions.size());

		var messages = new ArrayList<ConsumerRecord<String, String>>();

		var index = 0;
		for (var consumer : consumers) {
			var idx = index++;
			execSvc.submit(() -> {
				var pollResult = poll(idx, consumer);
				synchronized (messages) {
					addAll(messages, pollResult);
				}
			});
		}
		execSvc.shutdown();

		try {
			execSvc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.info("Executor shutdown was interrupted", e);
		}

		// Close all consumers
		destroyConsumers(consumers);

		logger.info("Fetched message count: {} (topic: {})", messages.size(), topic);
		return messages;
	}

	static void addAll(ArrayList<ConsumerRecord<String, String>> list, PollResult pollResult) {
		list.ensureCapacity(list.size() + pollResult.count());
		for (var recs : pollResult.recordsList()) {
			for (var r : recs) {
				list.add(r);
			}
		}
	}

	private record PollResult(List<ConsumerRecords<String, String>> recordsList, int count) {
	}

	PollResult poll(int index, Consumer<String, String> consumer) {
		if (logger.isInfoEnabled()) {
			logger.info("Consumer[{}] fetching messages from partitions: {}", index,
					TopicUtils.join(", ", consumer.assignment()));
		}

		var total = 0;
		var recordsList = new ArrayList<ConsumerRecords<String, String>>();
		while (true) {
			var records = consumer.poll(POLL_TIMEOUT);
			var count = records.count();
			logger.info("Consumer[{}] fetched message count: {}", index, count);
			if (count == 0) {
				break;
			}
			total += count;
			recordsList.add(records);
		}

		logger.info("Consumer[{}] total fetched message count: {}", index, total);
		return new PollResult(recordsList, total);
	}

}
