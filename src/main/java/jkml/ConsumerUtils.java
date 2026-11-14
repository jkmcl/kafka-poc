package jkml;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

public class ConsumerUtils {

	private ConsumerUtils() {
	}

	public static List<TopicPartition> partitionsFor(Consumer<?, ?> consumer, String topic) {
		var partitions = new ArrayList<TopicPartition>();
		consumer.partitionsFor(topic).forEach(i -> partitions.add(new TopicPartition(i.topic(), i.partition())));
		partitions.sort(Comparator.comparingInt(TopicPartition::partition));
		return partitions;
	}

	public static Map<TopicPartition, OffsetAndTimestamp> offsetsForTime(Consumer<?, ?> consumer,
			Iterable<TopicPartition> partitions, Instant time) {
		var times = new HashMap<TopicPartition, Long>();
		var ms = time.toEpochMilli();
		partitions.forEach(p -> times.put(p, ms));
		return consumer.offsetsForTimes(times);
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
