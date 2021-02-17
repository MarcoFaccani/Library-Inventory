package com.practice.libraryinventoryproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.libraryinventoryproducer.model.Book;
import com.practice.libraryinventoryproducer.model.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Here I test both methods after java 8 (lambdas) and prior java 8 (anonymous classes).
 * The first one is a void method, the second one returns a ListenableFuture;
 * This difference has major consequences when testing
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(OutputCaptureExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    @Spy
    private ObjectMapper objectMapper;


    // Positive test lambda method
    @Test
    public void sendLibraryEvent(CapturedOutput logs) throws JsonProcessingException {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(Book.builder()
                        .author("Patrick O'Brian")
                        .name("Master and Commander")
                        .id(346)
                        .build())
                .build();

        // create SendResult object
        String record = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getId(), record );
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);

        // create ListenableFuture with SendResult
        SettableListenableFuture future = new SettableListenableFuture();
        future.set(sendResult);

        when(kafkaTemplate.sendDefault(any(), any())).thenReturn(future);

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        // verify method handleSuccess has been invoked by verifying if log is printed
        assertThat(logs.getOut()).contains("Message LibraryEvent sent successfully");
    }

    // Positive test anonymous class method (prior java 8)
    @Test
    public void sendLibraryEvent_producerRecord(CapturedOutput logs) throws JsonProcessingException, ExecutionException, InterruptedException {

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(Book.builder()
                        .author("Patrick O'Brian")
                        .name("Master and Commander")
                        .id(346)
                        .build())
                .build();

        String record = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getId(), record );
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,342,System.currentTimeMillis(), 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);

        SettableListenableFuture future = new SettableListenableFuture();
        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        ListenableFuture<SendResult<Integer,String>> listenableFuture = libraryEventProducer.sendLibraryEventsWithHeaders_usingProducerRecord(libraryEvent);

        SendResult<Integer,String> sendResultResponse = listenableFuture.get();
        assertEquals(1, sendResultResponse.getRecordMetadata().partition());

        // verify method handleSuccess has been invoked by verifying if log is printed
        assertThat(logs.getOut()).contains("Message LibraryEvent sent successfully");
    }


    // Negative test lambda method
    @Test
    public void sendLibraryEvent_failure(CapturedOutput logs) throws JsonProcessingException {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(Book.builder()
                        .author("Patrick O'Brian")
                        .name("Master and Commander")
                        .id(346)
                        .build())
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.sendDefault(any(), any())).thenReturn(future);

        libraryEventProducer.sendLibraryEvent(libraryEvent);

        // verify method handleFailure has been invoked by verifying if log is printed
        assertThat(logs.getOut()).contains("Error sending LibraryEvent;");
    }


    // Negative test anonymous class method
    @Test
    public void sendLibraryEvent_failure_producerRecord() {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(Book.builder()
                        .author("Patrick O'Brian")
                        .name("Master and Commander")
                        .id(346)
                        .build())
                .build();

        SettableListenableFuture future = new SettableListenableFuture();
        future.setException(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class,
                () -> libraryEventProducer.sendLibraryEventsWithHeaders_usingProducerRecord(libraryEvent).get());

    }


}
