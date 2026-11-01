package jkml;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
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

	static <T> List<List<T>> subLists(List<T> list, int size) {
		List<List<T>> subLists = new ArrayList<>(size);
		for (var i = 0; i < size; ++i) {
			subLists.add(new ArrayList<>());
		}

		for (var i = 0; i < list.size(); ++i) {
			subLists.get(i % size).add(list.get(i));
		}

		return subLists;
	}

	public List<ConsumerRecord<String, String>> poll() {
		logger.info("Fetching messages from topic: {}", topic);

		var messages = new ArrayList<ConsumerRecord<String, String>>();

		fetchPartitions();

		try (var consumerGroup = new ConsumerGroup<>(consumerFactory)) {
			var consumerCount = Math.min(Runtime.getRuntime().availableProcessors(), partitions.size());
			var partitionsLists = subLists(partitions, consumerCount);
			for (var i = 0; i < consumerCount; ++i) {
				var index = i;
				consumerGroup.create(c -> c.assign(partitionsLists.get(index)));
			}

			var lock = new ReentrantLock();
			poll(consumerGroup, m -> {
				lock.lock();
				try {
					messages.ensureCapacity(messages.size() + m.count());
					m.forEach(messages::add);
				} finally {
					lock.unlock();
				}
			});
		}

		logger.info("Fetched message count: {}", messages.size());
		return messages;
	}

	void poll(ConsumerGroup<String, String> consumerGroup,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		// Prepare polling tasks
		var tasks = new ArrayList<Callable<Void>>();
		for (var i = 0; i < consumerGroup.size(); ++i) {
			var index = i;
			tasks.add(() -> {
				poll(index, consumerGroup.get(index), processor);
				return null;
			});
		}

		// Execute polling tasks
		var execSvc = Executors.newFixedThreadPool(consumerGroup.size());
		try {
			var futures = execSvc.invokeAll(tasks);
			var hasException = false;
			var index = 0;
			for (var future : futures) {
				hasException |= wasExceptionThrown(index++, future);
			}
			if (hasException) {
				throw new PollException("One or more concurrent polling tasks were terminated abnormally");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new PollException("The wait for concurrent polling tasks to complete was interrupted", e);
		} finally {
			execSvc.shutdownNow();
		}
	}

	boolean wasExceptionThrown(int index, Future<Void> future) {
		try {
			future.get();
			return false;
		} catch (InterruptedException e) {
			logger.error("Consumer[{}]: The wait for the polling task to complete was interrupted", index, e);
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			logger.error("Consumer[{}]: Fetching was aborted due to an exception", index, e);
		} catch (CancellationException e) {
			logger.error("Consumer[{}]: Fetching was cancelled", index, e);
		}
		return true;
	}

	void poll(int index, Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		if (logger.isInfoEnabled()) {
			logger.info("Consumer[{}]: Fetching messages from partition(s): {}", index,
					TopicUtils.join(", ", consumer.assignment()));
		}

		var batchCount = 0;
		var messageCount = 0;
		while (true) {
			var messages = consumer.poll(POLL_TIMEOUT);
			var count = messages.count();
			logger.debug("Consumer[{}]: Fetched {} message(s)", index, count);
			if (count == 0) {
				break;
			}
			++batchCount;
			messageCount += count;
			processor.accept(messages);
		}

		logger.debug("Consumer[{}]: Fetched {} message(s) in {} batch(es)", index, messageCount, batchCount);
	}

}
