package com.practice.libraryinventoryproducer.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile({"local", "dev"})
public class AutoCreateTopicsConfig {

    @Bean
    public NewTopic createEventLibraryTopic() {
        return TopicBuilder.name("library-events")
                .partitions(3)
                .replicas(3)
                .build();
    }

}
