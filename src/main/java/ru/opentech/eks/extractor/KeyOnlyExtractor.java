package ru.opentech.eks.extractor;

import org.apache.kafka.streams.KeyValue;

public class KeyOnlyExtractor<K, V> implements KeyValueExtractor<K, V, K> {

    @Override
    public K extractData( KeyValue<K, V> keyValue ) {
        return keyValue.key;
    }

}
