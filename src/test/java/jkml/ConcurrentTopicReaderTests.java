package jkml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
@EmbeddedKafka(topics = { "topic1" }, partitions = 4)
@DirtiesContext
class ConcurrentTopicReaderTests {

	private final Logger logger = LoggerFactory.getLogger(ConcurrentTopicReaderTests.class);

	@Autowired
	private ConsumerFactory<String, String> factory;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@BeforeEach
	void beforeEach(TestInfo testInfo) {
		logger.info("# Start of {}", testInfo.getDisplayName());
	}

	private record Message(String topic, String key, String value) {
	}

	private Message send() throws Exception {
		var str = UUID.randomUUID().toString();
		var msg = new Message("topic1", str, "Content " + str);
		logger.info("Sending message to topic: {}", msg.topic);
		kafkaTemplate.send(msg.topic, msg.key, msg.value).get();
		return msg;
	}

	@Test
	void testRead() throws Exception {
		var minTimestamp = Instant.now();
		var message = send();

		var reader = new ConcurrentTopicReader(factory::createConsumer, message.topic);
		var polledMessage = reader.read(minTimestamp).get(0);
		assertEquals(message.key, polledMessage.key());
		assertEquals(message.value, polledMessage.value());
	}

	@Test
	void testReadMany() throws Exception {
		var minTimestamp = Instant.now();
		var message = send();
		send();
		send();
		send();
		send();
		send();

		var reader = new ConcurrentTopicReader(factory::createConsumer, message.topic);
		var polledMessage = reader.read(minTimestamp);
		assertEquals(6, polledMessage.size());
	}

	@Test
	void testRead_notFound() throws Exception {
		var minTimestamp = Instant.now();
		var message = send();

		var reader = new ConcurrentTopicReader(factory::createConsumer, message.topic);
		assertTrue(reader.read(minTimestamp.plusSeconds(10)).isEmpty());
	}


	@Test
	void testWasExceptionThrown() throws Exception {
		var reader = new ConcurrentTopicReader(null, null);

		Future<Void> mockFuture = mock();
		when(mockFuture.get())
				.thenThrow(new ExecutionException(new IllegalStateException()))
				.thenThrow(new CancellationException())
				.thenThrow(new InterruptedException());

		assertTrue(reader.wasExceptionThrown(0, mockFuture));
		assertTrue(reader.wasExceptionThrown(0, mockFuture));
		assertTrue(reader.wasExceptionThrown(0, mockFuture));
	}

}
