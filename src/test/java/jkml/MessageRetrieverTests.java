package jkml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
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
	private ConsumerFactory<String, String> factory;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private Consumer<String, String> createConsumer() {
		return factory.createConsumer();
	}

	private void send(String topic, String key, String value) throws InterruptedException, ExecutionException {
		logger.info("Sending message to topic: {}", topic);
		kafkaTemplate.send(topic, key, value).get();
	}

	@BeforeEach
	void beforeEach(TestInfo testInfo) {
		logger.info("# Start of {}", testInfo.getDisplayName());
	}

	@Test
	void testPoll() throws Exception {
		var topic = UUID.randomUUID().toString();
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		try (var retriever = new MessageRetriever(createConsumer(), topic)) {
			var iter = retriever.poll().iterator();
			var msg = iter.next();
			assertEquals(key, msg.key());
			assertEquals(value, msg.value());
			assertFalse(iter.hasNext());
		}
	}

	@Test
	void testSeekToBeginning() throws Exception {
		var topic = UUID.randomUUID().toString();
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		try (var retriever = new MessageRetriever(createConsumer(), topic)) {
			retriever.poll(); // fetch once to move the offset
			retriever.seekToBeginning();
			assertFalse(retriever.poll().isEmpty());
		}
	}

	@Test
	void testSeekToEnd() throws Exception {
		var topic = UUID.randomUUID().toString();
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		try (var retriever = new MessageRetriever(createConsumer(), topic)) {
			retriever.seekToEnd();
			assertTrue(retriever.poll().isEmpty());
		}
	}

	@Test
	void testSeekToTime_found() throws Exception {
		var topic = UUID.randomUUID().toString();
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		try (var retriever = new MessageRetriever(createConsumer(), topic)) {
			retriever.seekToTime(Instant.now().minusSeconds(10));
			assertFalse(retriever.poll().isEmpty());
		}
	}

	@Test
	void testSeekToTime_notFound() throws Exception {
		var topic = UUID.randomUUID().toString();
		var key = "myKey";
		var value = UUID.randomUUID().toString();
		send(topic, key, value);

		try (var retriever = new MessageRetriever(createConsumer(), topic)) {
			retriever.seekToTime(Instant.now().plusSeconds(10));
			assertTrue(retriever.poll().isEmpty());
		}
	}

}
