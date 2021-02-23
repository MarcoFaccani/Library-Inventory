package com.practice.libraryinventoryconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.practice.libraryinventoryconsumer.consumer.LibraryConsumer;
import com.practice.libraryinventoryconsumer.entity.Book;
import com.practice.libraryinventoryconsumer.entity.LibraryEvent;
import com.practice.libraryinventoryconsumer.repository.LibraryInventoryRepository;
import com.practice.libraryinventoryconsumer.service.LibraryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    private LibraryConsumer libraryConsumerSpy;

    @SpyBean
    private LibraryService libraryServiceSpy;

    @Autowired
    private LibraryInventoryRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        //wait for all partitions to be assigned to the consumer before going on with the tests
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        //given
        String json = " {\"id\":null,\"type\":\"NEW\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        kafkaTemplate.sendDefault(json).get(); // sync produce

        // block thread for three seconds
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryConsumerSpy, times(1)).listener(isA(ConsumerRecord.class));
        verify(libraryServiceSpy, times(1)).processMessage(isA(String.class));

        List<LibraryEvent> libraryEventList = repository.findAll();

        assertEquals(1,libraryEventList.size());

        libraryEventList.forEach(libraryEvent -> {
            assertNotNull(libraryEvent.getId());
            assertEquals(456, libraryEvent.getBook().getId());
        });

    }


    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        String json = " {\"id\":null,\"type\":\"NEW\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);

        // sync produce message
        Book updatedBook = Book.builder().id(456).name("My Awesome Book").author("Marco").build();
        libraryEvent.setType(LibraryEvent.Type.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getId(), updatedJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryConsumerSpy, times(1)).listener(isA(ConsumerRecord.class));
        verify(libraryServiceSpy, times(1)).processMessage(isA(String.class));

        LibraryEvent persistedLibraryEvent = repository.findById(libraryEvent.getId()).get();
        assertEquals("My Awesome Book", persistedLibraryEvent.getBook().getName());
    }


    @Test
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {

        //given
        Integer libraryEventId = 123; // random number
        String json = "{\"id\":" + libraryEventId + ",\"type\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryConsumerSpy, atLeast(1)).listener(isA(ConsumerRecord.class));
        verify(libraryServiceSpy, atLeast(1)).processMessage(isA(String.class));

        Optional<LibraryEvent> libraryEventOptional = repository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }


    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"id\":" + libraryEventId + ",\"type\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // 1 time because IllegalArgumentException shall not kick retries
        verify(libraryConsumerSpy, atLeast(1)).listener(isA(ConsumerRecord.class));
        verify(libraryServiceSpy, atLeast(1)).processMessage(isA(String.class));
    }


    @Test
    void testRetriesWhenRecoverableDataAccessExceptionIsThrown() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"id\":" + libraryEventId + ",\"type\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // 3 times due to retries (see LibraryConsumerConfig)
        verify(libraryConsumerSpy, atLeast(3)).listener(isA(ConsumerRecord.class));
        verify(libraryServiceSpy, atLeast(3)).processMessage(isA(String.class));
    }



    @Test
    void testRecovery() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"id\":" + libraryEventId + ",\"type\":\"UPDATE\",\"book\":{\"id\":456,\"name\":\"My Awesome Book\",\"author\":\"Marco\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryConsumerSpy, atLeast(3)).listener(isA(ConsumerRecord.class));  // 3 times due to retries (see LibraryConsumerConfig)
        verify(libraryServiceSpy, atLeast(1)).handleRecovery(isA(ConsumerRecord.class));
    }

}
