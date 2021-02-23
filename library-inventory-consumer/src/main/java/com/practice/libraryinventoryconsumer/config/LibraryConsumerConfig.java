package com.practice.libraryinventoryconsumer.config;

import com.practice.libraryinventoryconsumer.service.LibraryService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@Configuration
@EnableKafka
@Slf4j
public class LibraryConsumerConfig {


    @Autowired
    private KafkaProperties properties;

    @Autowired
    private LibraryService libraryService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));

        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // manual acknowledge

        factory.setConcurrency(3); // create 3 istances of the consumer, helpful for local tests.

        // custom error handling
        factory.setErrorHandler( (thrownException, consumerRecord) -> {
            log.error("EXCEPTION: {}; \n Record: {}", thrownException.getMessage(), consumerRecord);
            //any custom logic, such as persist failed records or exceptions
        });

        // set retries logic
        factory.setRetryTemplate(retryTemplate());

        // set recovery logic
        factory.setRecoveryCallback( context -> {

            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                log.info("Record is recoverable");
                ConsumerRecord<Integer, String> consumerRecord =
                        (ConsumerRecord<Integer, String> ) context.getAttribute("record");

                libraryService.handleRecovery(consumerRecord);
            } else {
                log.info("Record is NOT recoverable");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        });

        return factory;
    }


    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000); //wait 1 second before each retry

        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);

        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        /*
        Retry if ANY exception is thrown
        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(3); // attempt to process failed record 3 times
         */

        /* Retry for only specific exceptions
         * if boolean is false do not retry
         * if boolean is true do retry
         */
        Map<Class<? extends Throwable>, Boolean>  exceptionsMap = new HashMap<>();
        exceptionsMap.put(IllegalArgumentException.class, false);
        exceptionsMap.put(RecoverableDataAccessException.class, true);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, exceptionsMap, true);
        return simpleRetryPolicy;
    }


}
