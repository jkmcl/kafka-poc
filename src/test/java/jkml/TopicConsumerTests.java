package jkml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

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
class TopicConsumerTests {

	private final Logger logger = LoggerFactory.getLogger(TopicConsumerTests.class);

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

	private Message send() throws InterruptedException, ExecutionException {
		var msg = new Message("topic1", "key1", "Content created at " + Instant.now().toString());
		logger.info("Sending message to topic: {}", msg.topic);
		kafkaTemplate.send(msg.topic, msg.key, msg.value).get();
		return msg;
	}

	@Test
	void testPoll() throws Exception {
		var message = send();

		var consumer = new TopicConsumer(message.topic, factory::createConsumer);
		var polledMessage = consumer.poll().get(0);
		assertEquals(message.key, polledMessage.key());
		assertEquals(message.value, polledMessage.value());
	}

	@Test
	void testCommitToTime_found() throws Exception {
		var message = send();

		var consumer = new TopicConsumer(message.topic, factory::createConsumer);
		consumer.commitToTime(Instant.now().minusSeconds(10));
		assertFalse(consumer.poll().isEmpty());
	}

	@Test
	void testCommitToTime_notFound() throws Exception {
		var message = send();

		var consumer = new TopicConsumer(message.topic, factory::createConsumer);
		consumer.commitToTime(Instant.now().plusSeconds(10));
		assertTrue(consumer.poll().isEmpty());
	}

}
