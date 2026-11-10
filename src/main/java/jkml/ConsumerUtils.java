package jkml;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerUtils {

	private ConsumerUtils() {
	}

	public static List<TopicPartition> partitionsFor(Consumer<?, ?> consumer, String topic) {
		return TopicUtils.toPartitions(consumer.partitionsFor(topic));
	}

	public static boolean hasLag(Consumer<?, ?> consumer) {
		for (var partition : consumer.assignment()) {
			var lag = consumer.currentLag(partition);
			if (lag.isEmpty() || lag.getAsLong() > 0) {
				return true;
			}
		}
		return false;
	}

}
