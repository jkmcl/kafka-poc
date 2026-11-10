package jkml;

import org.apache.kafka.clients.consumer.Consumer;

public class ConsumerUtils {

	private ConsumerUtils() {
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
