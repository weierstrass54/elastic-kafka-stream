package ru.opentech.eks.component;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerRecordHandler<K, V> {

    void apply( ConsumerRecord<K, V> record );

}
