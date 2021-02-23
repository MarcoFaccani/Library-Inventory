package com.practice.libraryinventoryconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practice.libraryinventoryconsumer.entity.LibraryEvent;
import com.practice.libraryinventoryconsumer.repository.LibraryInventoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Slf4j
public class LibraryService {

    @Autowired
    private LibraryInventoryRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    public void processMessage(final String message) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(message, LibraryEvent.class);
        log.info("libraryEvent : {} ", libraryEvent);

        // Only for demo purpose, this exception will trigger the retry attempt;
        // view the LibraryConsumerConfig's simpleRetryPolicy();
        if (libraryEvent.getId() != null && libraryEvent.getId() == 000)
            throw new RecoverableDataAccessException("database down");

        switch (libraryEvent.getType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
            }
            default -> log.error("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getId() == null)
            throw new IllegalArgumentException("Library Event ID is missing");

        repository.findById(libraryEvent.getId())
                .orElseThrow( () ->  new IllegalArgumentException("LibraryEvent not present in database"));

        log.info("Validation successful");
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("LibraryEvent saved");
    }


    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
        final Integer key = consumerRecord.key();
        final String value = consumerRecord.value();
        kafkaTemplate.sendDefault(key, value)
                     .addCallback(
                            result -> log.info("Recovery message sent successfully; key: {}, value: {}, partition: {}",
                                    key, value, consumerRecord.partition() ),
                            ex ->  log.error("Error sending Recovery message; key: {}, value: {}, exception: {}", key, value, ex.getMessage())
                     );
    }

}
