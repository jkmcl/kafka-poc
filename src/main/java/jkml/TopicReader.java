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

	public List<ConsumerRecord<String, String>> poll() {
		logger.info("Fetching messages from topic: {}", topic);

		getPartitions();

		try (var consumer = consumerFactory.get()) {
			consumer.assign(partitions);
			var pollResult = poll(consumer);
			var messages = new ArrayList<ConsumerRecord<String, String>>(pollResult.count());
			for (var recs : pollResult.recordsList()) {
				for (var r : recs) {
					messages.add(r);
				}
			}
			return messages;
		}
	}

	private record PollResult(List<ConsumerRecords<String, String>> recordsList, int count) {
	}

	PollResult poll(Consumer<String, String> consumer) {
		if (logger.isInfoEnabled()) {
			logger.info("Fetching messages from partitions: {}", TopicUtils.join(", ", consumer.assignment()));
		}

		var total = 0;
		var recordsList = new ArrayList<ConsumerRecords<String, String>>();
		while (true) {
			var records = consumer.poll(POLL_TIMEOUT);
			var count = records.count();
			logger.info("Fetched message count: {}", count);
			if (count == 0) {
				break;
			}
			total += count;
			recordsList.add(records);
		}

		logger.info("Total fetched message count: {}", total);
		return new PollResult(recordsList, total);
	}

}
