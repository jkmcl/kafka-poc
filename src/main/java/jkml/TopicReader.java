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
		if (partitions.isEmpty()) {
			return messages;
		}

		try (var consumer = consumerFactory.get()) {
			consumer.assign(partitions);
			poll(consumer, m -> {
				messages.ensureCapacity(messages.size() + m.count());
				m.forEach(messages::add);
			});
			return messages;
		}
	}

	void poll(Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		if (logger.isInfoEnabled()) {
			logger.info("Fetching messages from partitions: {}", TopicUtils.join(", ", consumer.assignment()));
		}

		var total = 0;
		var elapsedTimes = new ElapsedTimes();
		var stopwatch = new Stopwatch();
		while (true) {
			// Fetch
			stopwatch.start();
			var records = consumer.poll(POLL_TIMEOUT);
			var elapsed1 = stopwatch.elapsed();

			var count = records.count();
			if (count == 0) {
				break;
			}
			total += count;

			// Process
			stopwatch.start();
			processor.accept(records);
			var elapsed2 = stopwatch.elapsed();

			logger.info("Read {} messages (fetched in {}, processed in {})", count, elapsed1, elapsed2);
			elapsedTimes.addFetch(elapsed1).addProcess(elapsed2);
		}

		logger.info("Total message count: {} (fetched in {}, processed in {})", total, elapsedTimes.getFetch(),
				elapsedTimes.getProcess());
	}

}
