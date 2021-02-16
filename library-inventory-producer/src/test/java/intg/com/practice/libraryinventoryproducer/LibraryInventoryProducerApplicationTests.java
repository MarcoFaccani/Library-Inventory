package com.practice.libraryinventoryproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.libraryinventoryproducer.model.Book;
import com.practice.libraryinventoryproducer.model.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
									"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"} )
class LibraryInventoryProducerApplicationTests {

	@Autowired
	private TestRestTemplate testRestTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	private ObjectMapper objectMapper;

	private Consumer<Integer, String> consumer;

	// Setup consumer
	@BeforeEach
	void setup() {
		Map<String, Object> configs = new HashMap(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}

	// turn off consumer
	@AfterEach
	void tearDown() {
		consumer.close();
	}

	@Test
	@Timeout(5)
	void postLibraryEvent() throws JsonProcessingException {
		LibraryEvent libraryEvent = LibraryEvent.builder()
				.id(null)
				.type(LibraryEvent.Type.NEW)
				.book(Book.builder()
						.author("Patrick O'Brian")
						.name("Master and Commander")
						.id(346)
						.build())
				.build();

		// call API to write on Topic
		ResponseEntity<HttpStatus> response =
				testRestTemplate.exchange("/api/v1/library-event", HttpMethod.POST, new HttpEntity<>(libraryEvent), HttpStatus.class);

		assertEquals(HttpStatus.CREATED, response.getStatusCode());

		// consume message from topic
		ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

		// verify the message received by the consumer is as expected
		assertEquals(libraryEvent, objectMapper.readValue(consumerRecord.value(), LibraryEvent.class));
	}

}
