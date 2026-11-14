package jkml;

import java.time.Instant;
import java.util.ArrayList;
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

public final class ConcurrentTopicReader extends SimpleTopicReader {

	public ConcurrentTopicReader(Supplier<Consumer<String, String>> consumerFactory, String topic) {
		super(consumerFactory, topic);
	}

	@Override
	public List<ConsumerRecord<String, String>> read(Instant minTimestamp) {
		var lock = new ReentrantLock();
		var messages = new ArrayList<ConsumerRecord<String, String>>();
		process(minTimestamp, m -> {
			lock.lock();
			try {
				messages.ensureCapacity(messages.size() + m.count());
				m.forEach(messages::add);
			} finally {
				lock.unlock();
			}

		});
		return messages;
	}

	@Override
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

	boolean wasExceptionThrown(int index, Future<Void> future) {
		var prefix = index < 0 ? "" : "Consumer[%s]: ".formatted(index);
		try {
			future.get();
			return false;
		} catch (InterruptedException e) {
			logger.error("{}The wait for the polling task to complete was interrupted", prefix, e);
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			logger.error("{}Fetching was aborted due to an exception", prefix, e);
		} catch (CancellationException e) {
			logger.error("{}Fetching was cancelled", prefix, e);
		}
		return true;
	}

}
