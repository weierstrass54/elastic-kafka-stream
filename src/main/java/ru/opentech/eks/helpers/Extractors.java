package ru.opentech.eks.helpers;

import ru.opentech.eks.extractor.KeyValueExtractor;

public class Extractors {

    public static <K> KeyValueExtractor<K, ?, K> keyOnlyExtractor() {
        return keyValue -> keyValue.key;
    }

    public static <V> KeyValueExtractor<?, V, V> valueOnlyExtractor() {
        return keyValue -> keyValue.value;
    }

    private Extractors() {
        throw new UnsupportedOperationException( "Утилитный класс" );
    }

}
