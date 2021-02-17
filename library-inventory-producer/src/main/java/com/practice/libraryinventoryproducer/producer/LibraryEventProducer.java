package com.practice.libraryinventoryproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.libraryinventoryproducer.model.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaFailureCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


// Here you will find common different ways to produce a message, for demonstration purpose
@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;


    // async produce
    public void sendLibraryEvent(final LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(key, value).addCallback(
                result -> handleSuccess(key, value, result),
                ex -> handleFailure(key, value, ex)
        );
    }


    // sync produce
    public void sendLibraryEventSync(final LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        try {
            kafkaTemplate.sendDefault(key, value).get(3, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException ex) {
            log.error("ExecutionException/InterruptedException sending LibraryEvent; key: {}, value: {}, exception: {}", key, value, ex.getMessage());
        } catch (Exception ex) {
            log.error("Exception sending LibraryEvent; key: {}, value: {}, exception: {}", key, value, ex.getMessage());
        }
    }


    // async produce using ProducerRecord and no lambda expression (java 7-)
    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventsWithHeaders_usingProducerRecord(final LibraryEvent libraryEvent) throws JsonProcessingException {
        final String topic = "library-events";
        final Integer key = libraryEvent.getId();
        final String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(buildProducerRecord(key, value, topic));

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

        return listenableFuture;
    }




    public ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message LibraryEvent sent successfully; key: {}, value: {}, partition: {}",
                key, value, result.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending LibraryEvent; key: {}, value: {}, exception: {}", key, value, ex.getMessage());
    }


}
