package com.practice.libraryinventoryconsumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * If you would like to try it out, you will need to uncomment the @Component annotation and comment that same annotation
 * in the LibraryConsumer. Also, uncomment the code block in the LibraryConsumerConfig.
 */

//@Component
@Slf4j
public class ConsumerManualAcknowledge implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = "${custom.kafka.topic}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("NEW MESSAGE | ConsumerRecord: {}", consumerRecord);
        acknowledgment.acknowledge();
    }

}
