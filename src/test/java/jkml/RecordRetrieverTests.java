package jkml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * See https://docs.spring.io/spring-kafka/reference/testing.html
 */
@SpringBootTest
@EmbeddedKafka(kraft = true, brokerProperties = { "auto.create.topics.enable=true" })
@DirtiesContext
class RecordRetrieverTests {

	private final Logger logger = LoggerFactory.getLogger(RecordRetrieverTests.class);

	@Autowired
	private RecordRetriever retriever;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private void send(String topic, String key, String value) throws InterruptedException, ExecutionException {
		logger.info("Sending message to topic: {}", topic);
		kafkaTemplate.send(topic, key, value).get();
	}

	@Test
	void testPoll() throws Exception {
		var topic = "topic1";
		var key = "myKey";
		var value = "myValue";
		send(topic, key, value);

		var iter = retriever.poll(topic).iterator();
		var msg = iter.next();
		assertEquals(key, msg.key());
		assertEquals(value, msg.value());
		assertFalse(iter.hasNext());

		logger.info("Timestamp: {}", Instant.ofEpochMilli(msg.timestamp()));
	}

	@Test
	void testPollFromBeginning() throws Exception {
		var topic = "topic2";
		var key = "myKey";
		var value = "myValue";
		send(topic, key, value);

		retriever.poll(topic).iterator();

		var iter = retriever.pollFromBeginning(topic).iterator();
		var msg = iter.next();
		assertEquals(key, msg.key());
		assertEquals(value, msg.value());
		assertFalse(iter.hasNext());
	}

}
