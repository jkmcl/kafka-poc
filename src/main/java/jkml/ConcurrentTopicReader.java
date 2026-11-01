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

	static <T> List<List<T>> splitList(List<T> list, int subListCount) {
		List<List<T>> subLists = new ArrayList<>(subListCount);
		for (var i = 0; i < subListCount; ++i) {
			subLists.add(new ArrayList<>());
		}

		for (int i = 0, n = list.size(); i < n; ++i) {
			subLists.get(i % subListCount).add(list.get(i));
		}

		return subLists;
	}

	public List<ConsumerRecord<String, String>> poll() throws InterruptedException {
		logger.info("Fetching messages from topic: {}", topic);

		var messages = new ArrayList<ConsumerRecord<String, String>>();

		fetchPartitions();
		if (partitions.isEmpty()) {
			return messages;
		}

		// Determine thread count
		var threadCount = Math.min(Runtime.getRuntime().availableProcessors(), partitions.size());
		try (var consumers = new ConsumerGroup<String, String>()) {
			// Create consumers and assign partitions to them
			var partitionLists = splitList(partitions, threadCount);
			for (var i = 0; i < threadCount; ++i) {
				var con = consumers.add(consumerFactory); // NOPMD: consumers are managed by ConsumerGroup
				var list = partitionLists.get(i);
				if (logger.isDebugEnabled()) {
					logger.debug("Assigning partitions to consumer[{}]: {}", i, TopicUtils.join(", ", list));
				}
				con.assign(list);
			}

			// Prepare polling tasks
			var lock = new ReentrantLock();
			var tasks = new ArrayList<Callable<Void>>();
			for (var i = 0; i < threadCount; ++i) {
				var index = i;
				tasks.add(() -> {
					poll(index, consumers.get(index), m -> {
						lock.lock();
						try {
							messages.ensureCapacity(messages.size() + m.count());
							m.forEach(messages::add);
						} finally {
							lock.unlock();
						}
					});
					return null;
				});
			}

			// Execute polling tasks
			var execSvc = Executors.newFixedThreadPool(threadCount);
			try {
				validateDoneFutures(execSvc.invokeAll(tasks));
				logger.info("Fetched message count: {} (topic: {})", messages.size(), topic);
				return messages;
			} finally {
				execSvc.shutdownNow();
			}
		}
	}

	<T> void validateDoneFutures(List<Future<T>> futures) {
		var hasException = false;
		for (int i = 0, n = futures.size(); i < n; ++i) {
			try {
				futures.get(i).get();
				continue;
			} catch (InterruptedException e) {
				logger.error("Consumer[{}]: Fetching result is unknown due to current thread being interrupted while waiting", i, e);
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				logger.error("Consumer[{}]: Fetching was aborted due to an exception", i, e.getCause());
			} catch (CancellationException e) {
				logger.error("Consumer[{}]: Fetching was cancelled", i, e);
			}
			hasException = true;
		}
		if (hasException) {
			throw new PollException("One or more concurrent polling task was not completed normally");
		}
	}

	void poll(int index, Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		if (logger.isInfoEnabled()) {
			logger.info("Consumer[{}]: Fetching messages from partitions: {}", index, TopicUtils.join(", ", consumer.assignment()));
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

			logger.info("Consumer[{}]: Read {} messages (fetched in {}, processed in {})", index, count, elapsed1, elapsed2);
			elapsedTimes.addFetch(elapsed1).addProcess(elapsed2);
		}

		logger.info("Consumer[{}]: Total message count: {} (fetched in {}, processed in {})", index, total,
				elapsedTimes.getFetch(), elapsedTimes.getProcess());
	}

}
