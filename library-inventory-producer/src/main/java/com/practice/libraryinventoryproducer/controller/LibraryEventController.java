package com.practice.libraryinventoryproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practice.libraryinventoryproducer.model.LibraryEvent;
import com.practice.libraryinventoryproducer.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequestMapping("/api/v1")
@RestController
public class LibraryEventController {

    @Autowired
    private LibraryEventProducer libraryEventProducer;

    @PostMapping("/library-event")
    public ResponseEntity<HttpStatus> postLibraryEvent(@Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("New postLibraryEvent - timestamp: {}", ZonedDateTime.now().toString());

        libraryEvent.setType(LibraryEvent.Type.NEW);
        libraryEventProducer.sendLibraryEvent(libraryEvent);

        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @PutMapping("/library-event")
    public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("New putLibraryEvent - timestamp: {}", ZonedDateTime.now().toString());

        if (libraryEvent.getId()==null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("LibraryEvent's ID is required");
        }

        libraryEvent.setType(LibraryEvent.Type.UPDATE);
        libraryEventProducer.sendLibraryEvent(libraryEvent);
        return new ResponseEntity<>(HttpStatus.OK);
    }


}
