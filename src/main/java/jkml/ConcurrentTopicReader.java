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

	public List<ConsumerRecord<String, String>> poll(Instant minTimestamp) {
		logger.info("Fetching messages from topic: {}", topic);

		var messages = new ArrayList<ConsumerRecord<String, String>>();

		commitToTime(minTimestamp);

		var lock = new ReentrantLock();
		poll(m -> {
			lock.lock();
			try {
				messages.ensureCapacity(messages.size() + m.count());
				m.forEach(messages::add);
			} finally {
				lock.unlock();
			}
		});

		logger.info("Fetched message count: {}", messages.size());
		return messages;
	}

	void poll(java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		// Prepare polling tasks
		var threadCount = Math.min(Runtime.getRuntime().availableProcessors(), partitions.size());
		var tasks = new ArrayList<Callable<Void>>(threadCount);
		var partitionsLists = subLists(partitions, threadCount);
		for (var i = 0; i < threadCount; ++i) {
			var index = i;
			tasks.add(() -> {
				try (var consumer = consumerFactory.get()) {
					consumer.assign(partitionsLists.get(index));
					poll(index, consumer, processor);
				}
				return null;
			});
		}

		// Execute polling tasks
		var execSvc = Executors.newFixedThreadPool(threadCount);
		try {
			var index = 0;
			var hasException = false;
			for (var future : execSvc.invokeAll(tasks)) {
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
		if (logger.isDebugEnabled()) {
			logger.debug("Consumer[{}]: Fetching messages from partition(s): {}", index,
					TopicUtils.join(", ", consumer.assignment()));
		}

		var batchCount = 0;
		var messageCount = 0;
		do {
			var messages = consumer.poll(POLL_TIMEOUT);
			++batchCount;
			var count = messages.count();
			logger.debug("Consumer[{}]: Fetched {} message(s)", index, count);
			messageCount += count;
			processor.accept(messages);
		} while (ConsumerUtils.hasLag(consumer));

		logger.debug("Consumer[{}]: Fetched {} message(s) in {} batch(es)", index, messageCount, batchCount);
	}

}
