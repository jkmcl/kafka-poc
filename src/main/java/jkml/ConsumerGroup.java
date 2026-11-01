package jkml;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroup<K, V> implements Closeable {

	private final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);

	private final List<Consumer<K, V>> consumers = new ArrayList<>();

	public Consumer<K, V> add(Supplier<Consumer<K, V>> consumerFactory) {
		var consumer = consumerFactory.get();
		consumers.add(consumer);
		return consumer;
	}

	public Consumer<K, V> get(int index) {
		return consumers.get(index);
	}

	public int size() {
		return consumers.size();
	}

	@Override
	public void close() {
		var index = 0;
		for (var consumer : consumers) { // NOPMD: the consumer is closed in the loop explicitly
			try {
				consumer.close();
				++index;
			} catch (Exception e) {
				logger.warn("Failed to close consumer[{}]", index, e);
			}
		}
	}

}
