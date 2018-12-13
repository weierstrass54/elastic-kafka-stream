package ru.opentech.eks.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.opentech.eks.component.KafkaListenerFactory;
import ru.opentech.eks.component.KafkaStreamFactory;

@Configuration
public class KafkaConfig {

    @Value( "${kafka.bootstrap-servers}" )
    private String bootstrapServers;

    @Value( "${kafka.consumer.group-id}" )
    private String groupId;

    @Value( "${kafka.consumer.auto-commit:true}" )
    private boolean autoCommit;

    @Value( "${kafka.consumer.commit-interval-ms:1000}" )
    private int autoCommitIntervalMs;

    @Bean
    public KafkaListenerFactory getKafkaListenerFactory() {
        return new KafkaListenerFactory(
            bootstrapServers, groupId, autoCommit, autoCommitIntervalMs
        );
    }

    @Bean
    public KafkaStreamFactory getKafkaStreamFactory() {
        return new KafkaStreamFactory( bootstrapServers );
    }

}
