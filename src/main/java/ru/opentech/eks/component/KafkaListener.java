package ru.opentech.eks.component;

import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class KafkaListener<K, V> {

    private static final Duration DEFAULT_POLL_DURATION = Duration.ofMillis( 100 );

    private Collection<String> topics;
    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    private ConsumerRecordHandler<K, V> handler;
    private Duration pollTimeout;

    public KafkaListener() {
        this( DEFAULT_POLL_DURATION );
    }

    public KafkaListener( Duration pollTimeout ) {
        this.pollTimeout = pollTimeout;
    }

    public Collection<String> getTopics() {
        return Optional.ofNullable( topics ).orElse( Collections.emptyList() );
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public Deserializer<K> getKeyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> getValueDeserializer() {
        return valueDeserializer;
    }

    public ConsumerRecordHandler<K, V> getHandler() {
        return handler;
    }

    public KafkaListener<K, V> topics( String... topics ) {
        this.topics = Arrays.asList( topics );
        return this;
    }

    public KafkaListener<K, V> keyDeserializer( Deserializer<K> keyDeserializer ) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    public KafkaListener<K, V> valueDeserializer( Deserializer<V> valueDeserializer ) {
        this.valueDeserializer = valueDeserializer;
        return this;
    }

    public KafkaListener<K, V> flatMap( ConsumerRecordHandler<K, V> handler ) {
        this.handler = handler;
        return this;
    }

    @Override
    public String toString() {
        return "{KafkaListener" + super.toString() + ", topics = [ " + String.join( ", ", topics ) + " ]}";
    }

}
