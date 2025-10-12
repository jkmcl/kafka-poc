package jkml;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Component
public class MessageRetriever {

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private final Logger logger = LoggerFactory.getLogger(MessageRetriever.class);

	private final ConsumerFactory<String, String> consumerFactory;

	public MessageRetriever(ConsumerFactory<String, String> consumerFactory) {
		this.consumerFactory = consumerFactory;
	}

	private List<TopicPartition> getPartitions(Consumer<String, String> consumer, String topic) {
		var info = consumer.partitionsFor(topic, TIMEOUT);
		var partitions = new ArrayList<TopicPartition>(info.size());
		info.forEach(pi -> partitions.add(new TopicPartition(topic, pi.partition())));
		return partitions;
	}

	public Iterable<ConsumerRecord<String, String>> poll(String topic) {
		logger.info("Fetching messages from topic: {}", topic);
		try (var consumer = consumerFactory.createConsumer()) {
			consumer.assign(getPartitions(consumer, topic));
			return consumer.poll(TIMEOUT).records(topic);
		}
	}

	public Iterable<ConsumerRecord<String, String>> pollFromBeginning(String topic) {
		logger.info("Fetching messages from beginning from topic: {}", topic);
		try (var consumer = consumerFactory.createConsumer()) {
			var partitions = getPartitions(consumer, topic);
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			return consumer.poll(TIMEOUT).records(topic);
		}
	}

}
