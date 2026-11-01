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

	private final Supplier<Consumer<K, V>> consumerFactory;

	public ConsumerGroup(Supplier<Consumer<K, V>> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	public void create(java.util.function.Consumer<Consumer<K, V>> customizer) {
		var consumer = consumerFactory.get(); // NOPMD: the consumer will be closed by close()
		consumers.add(consumer);
		customizer.accept(consumer);
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
			} catch (Exception e) {
				logger.warn("Failed to close consumer[{}]", index, e);
			}
			++index;
		}
		consumers.clear();
	}

}
