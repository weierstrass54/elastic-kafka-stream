package ru.opentech.eks.repository;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import ru.opentech.eks.component.KafkaStreamFactory;
import ru.opentech.eks.extractor.KeyValueExtractor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class KafkaRepository<K, V> {

    private final ReadOnlyKeyValueStore<K, V> store;

    protected KafkaRepository(
        KafkaStreamFactory kafkaStreamFactory,
        String storeName
    ) {
        this.store = kafkaStreamFactory.startStream( topology() ).store( storeName, QueryableStoreTypes.keyValueStore() );
    }

    public <T> List<T> loadList( KeyValueExtractor<K, V, T> extractor ) {
        List<T> result = new ArrayList<>();
        KeyValueIterator<K, V> iterator = store.all();
        while( iterator.hasNext() ) {
            result.add( extractor.extractData( iterator.next() ) );
        }
        return result;
    }

    public <T> T loadObject( KeyValueExtractor<K, V, T> extractor ) {
        return head( loadList( extractor ) );
    }

    protected abstract Topology topology();

    protected static <T> T head( Iterable<T> iterable ) {
        Iterator<T> it = iterable.iterator();
        if( !it.hasNext() ) {
            throw new RuntimeException( "Incorrect result size: expected more than 0." );
        }
        return it.next();
    }
}
