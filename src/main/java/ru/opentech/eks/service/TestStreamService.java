package ru.opentech.eks.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.opentech.eks.Application;
import ru.opentech.eks.component.KafkaStreamFactory;
import ru.opentech.eks.helpers.JsonHelper;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class TestStreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    private KafkaStreamFactory streamFactory;

    @Autowired
    public void setStreamFactory( KafkaStreamFactory streamFactory ) {
        this.streamFactory = streamFactory;
    }

    public void start() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream( "catalog_offers_refresh-production", Consumed.with( STRING_SERDE, STRING_SERDE ) )
            .flatMap( ( key, value ) -> {
                Event ev = JsonHelper.read( value, Event.class );
                if( ev != null ) {
                    return ev.getReferenceIds().stream()
                        .map( id -> new KeyValue<>( id, id ) )
                        .collect( Collectors.toList() );
                }
                else {
                    return Collections.emptyList();
                }
            } )
            .groupByKey(
                Grouped.with( INTEGER_SERDE, INTEGER_SERDE )
            )
            .count(
                Materialized.<Integer, Long, KeyValueStore<Bytes, byte[]>>as( Application.STREAM_NAME )
                    .withKeySerde( INTEGER_SERDE ).withValueSerde( LONG_SERDE )
            )
            .toStream()
            .to( Application.STREAM_NAME, Produced.with( INTEGER_SERDE, LONG_SERDE ) );
        streamFactory.startStream( builder.build() );
    }

    @JsonIgnoreProperties( ignoreUnknown = true )
    private static class Event {

        @JsonProperty( "keys" )
        EventData keys;

        List<Integer> getReferenceIds() {
            if( keys == null ) {
                return Collections.emptyList();
            }
            return keys.ids;
        }

        private static class EventData {

            @JsonProperty( "id" )
            List<Integer> ids;

        }

    }

}
