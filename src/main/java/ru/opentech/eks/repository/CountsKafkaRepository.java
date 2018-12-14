package ru.opentech.eks.repository;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import ru.opentech.eks.component.KafkaStreamFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class CountsKafkaRepository extends KafkaRepository {

    private KeyValueStore keyValueStore;

    @Autowired
    public CountsKafkaRepository( KafkaStreamFactory kafkaStreamFactory ) {
        super( kafkaStreamFactory );
    }

    public List<Map<Integer, Long>> findAll() {
        /*
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
        */

        /*
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
         */

        List<Map<Integer, Long>> rows = new ArrayList<>();
        KeyValueIterator<Integer, Long> it = keyValueStore.all();
        while( it.hasNext() ) {
            KeyValue<Integer, Long> next = it.next();
            Map<Integer, Long> item = new HashMap<>();
            item.put( next.key, next.value );
            rows.add( item );
        }
        return rows;
    }

    public Map<Integer, Long> findById( int id ) {
        /*
        ReadOnlyKeyValueStore<String, Long> keyValueStore =
    streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());

// Get value by key
System.out.println("count for hello:" + keyValueStore.get("hello"));
         */
    }

}
