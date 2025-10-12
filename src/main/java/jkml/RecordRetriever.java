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
public class RecordRetriever {

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private final Logger logger = LoggerFactory.getLogger(RecordRetriever.class);

	private final ConsumerFactory<String, String> consumerFactory;

	public RecordRetriever(ConsumerFactory<String, String> consumerFactory) {
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
			var consumerRecords = consumer.poll(Duration.ofSeconds(5));
			return consumerRecords.records(topic);
		}
	}

	public Iterable<ConsumerRecord<String, String>> pollFromBeginning(String topic) {
		logger.info("Fetching messages from beginning from topic: {}", topic);
		try (var consumer = consumerFactory.createConsumer()) {
			var partitions = getPartitions(consumer, topic);
			consumer.assign(partitions);
			consumer.seekToBeginning(partitions);
			var consumerRecords = consumer.poll(Duration.ofSeconds(5));
			return consumerRecords.records(topic);
		}
	}

}
