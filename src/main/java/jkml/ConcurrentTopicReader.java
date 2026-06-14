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

	public static <T> List<List<T>> splitList(List<T> originalList, int subListCount) {
		if (subListCount <= 0) {
			throw new IllegalArgumentException("Number of sublists must be greater than 0");
		}

		List<List<T>> subLists = new ArrayList<>(subListCount);
		for (var i = 0; i < subListCount; ++i) {
			subLists.add(new ArrayList<>());
		}

		for (int i = 0, size = originalList.size(); i < size; ++i) {
			subLists.get(i % subListCount).add(originalList.get(i));
		}

		return subLists;
	}

	private List<Consumer<String, String>> createConsumers() {
		var procCount = Runtime.getRuntime().availableProcessors();
		logger.debug("Processor count: {}", procCount);

		var conCount = Math.min(procCount * 2, partitions.size());
		logger.debug("Creating {} consumer(s)", conCount);
		var consumers = new ArrayList<Consumer<String, String>>(conCount);
		for (var i = 0; i < conCount; ++i) {
			consumers.add(consumerFactory.get());
		}

		var partitionLists = splitList(partitions, conCount);
		for (var i = 0; i < conCount; ++i) {
			var list = partitionLists.get(i);
			if (logger.isDebugEnabled()) {
				logger.debug("Assigning partitions to consumer[{}]: {}", i, TopicUtils.join(", ", list));
			}
			consumers.get(i).assign(list);
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
				logger.warn("Failed to close consumer[{}]", index, e);
			}
		}
	}

	public List<ConsumerRecord<String, String>> poll() throws InterruptedException {
		logger.info("Fetching messages from topic: {}", topic);

		getPartitions();

		var consumers = createConsumers();
		var collector = new MessageCollector();

		var tasks = new ArrayList<Callable<Void>>();
		var index = 0;
		for (var consumer : consumers) {
			var idx = index++;
			tasks.add(() -> {
				poll(idx, consumer, collector::add);
				return null;
			});
		}

		var execSvc = Executors.newFixedThreadPool(consumers.size());
		try {
			if (validateDoneFutures(execSvc.invokeAll(tasks))) {
				logger.info("Fetched message count: {} (topic: {})", collector.count(), topic);
				var result = new ArrayList<ConsumerRecord<String, String>>(collector.count());
				collector.export(result::add);
				return result;
			} else {
				throw new PollException("One or more concurrent polling task was not completed");
			}
		} finally {
			execSvc.shutdownNow();
			destroyConsumers(consumers);
		}
	}

	<T> boolean validateDoneFutures(List<Future<T>> futures) {
		var result = true;
		for (int i = 0, size = futures.size(); i < size; i++) {
			var f = futures.get(i);
			try {
				f.get();
				continue;
			} catch (InterruptedException e) {
				logger.error("Consumer[{}]: Fetching result is unknown due to current thread being interrupted while waiting", i, e);
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				logger.error("Consumer[{}]: Fetching was aborted due to an exception", i, e.getCause());
			} catch (CancellationException e) {
				logger.error("Consumer[{}]: Fetching was cancelled", i, e);
			}
			result = false;
		}
		return result;
	}

	void poll(int index, Consumer<String, String> consumer,
			java.util.function.Consumer<ConsumerRecords<String, String>> processor) {
		if (logger.isInfoEnabled()) {
			logger.info("Consumer[{}] Fetching messages from partitions: {}", index,
					TopicUtils.join(", ", consumer.assignment()));
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

			logger.info("Consumer[{}]: Read {} messages (fetched in {}, processed in {})", index, count, elapsed1,
					elapsed2);
			elapsedTimes.addFetch(elapsed1).addProcess(elapsed2);
		}

		logger.info("Consumer[{}]: Total message count: {} (fetched in {}, processed in {})", index, total, elapsedTimes.getFetch(), elapsedTimes.getProcess());
	}

}
