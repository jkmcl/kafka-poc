package jkml;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerGroup implements AutoCloseable {

	private final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);

	private final List<Consumer<String, String>> consumers = new ArrayList<>();

	public ConsumerGroup(Supplier<Consumer<String, String>> consumerFactory, int size) {
		for (var i = 0; i < size; ++i) {
			consumers.add(consumerFactory.get());
		}
	}

	public Consumer<String, String> get(int index) {
		return consumers.get(index);
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
