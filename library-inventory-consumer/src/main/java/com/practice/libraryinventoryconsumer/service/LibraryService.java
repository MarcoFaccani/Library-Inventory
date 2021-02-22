package com.practice.libraryinventoryconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practice.libraryinventoryconsumer.entity.LibraryEvent;
import com.practice.libraryinventoryconsumer.repository.LibraryInventoryRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;

@Service
@Slf4j
public class LibraryService {

    @Autowired
    private LibraryInventoryRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    public void processMessage(final String message) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(message, LibraryEvent.class);
        log.info("libraryEvent : {} ", libraryEvent);

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


}
