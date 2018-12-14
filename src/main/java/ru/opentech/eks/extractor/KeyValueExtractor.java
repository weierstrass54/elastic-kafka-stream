package ru.opentech.eks.extractor;

import org.apache.kafka.streams.KeyValue;

import java.util.List;

@FunctionalInterface
public interface KeyValueExtractor<K, V, R> {

    R extractData(KeyValue<K, V> keyValue );

}
