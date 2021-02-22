package com.practice.libraryinventoryconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.practice.libraryinventoryconsumer.service.LibraryService;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryConsumer {

    @Autowired
    private LibraryService libraryService;

    @KafkaListener(topics = "${custom.kafka.topic}")
    public void listener(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("NEW MESSAGE | consumerRecord: {}", consumerRecord);
        libraryService.processMessage(consumerRecord.value());
    }

}


