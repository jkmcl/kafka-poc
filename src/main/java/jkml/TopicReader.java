package jkml;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface TopicReader {

	List<ConsumerRecord<String, String>> read(Instant minTimestamp);

	void process(Instant minTimestamp, Consumer<ConsumerRecords<String, String>> processor);

}
