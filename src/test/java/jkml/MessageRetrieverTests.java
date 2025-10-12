package jkml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
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
class MessageRetrieverTests {

	private final Logger logger = LoggerFactory.getLogger(MessageRetrieverTests.class);

	@Autowired
	private MessageRetriever retriever;

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
		var value = UUID.randomUUID().toString();
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
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		retriever.poll(topic).iterator(); // fetch once to move the offset

		var iter = retriever.pollFromBeginning(topic).iterator();
		var msg = iter.next();
		assertEquals(key, msg.key());
		assertEquals(value, msg.value());
		assertFalse(iter.hasNext());
	}

	@Test
	void testPollFromTimestamp_found() throws Exception {
		var topic = "topic3";
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		var iter = retriever.pollFromTimestamp(topic, Instant.now().minusSeconds(10)).iterator();
		assertTrue(iter.hasNext());
	}

	@Test
	void testPollFromTimestamp_notFound() throws Exception {
		var topic = "topic4";
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		var iter = retriever.pollFromTimestamp(topic, Instant.now().plusSeconds(10)).iterator();
		assertFalse(iter.hasNext());
	}

}
