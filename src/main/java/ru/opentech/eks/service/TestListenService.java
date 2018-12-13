package ru.opentech.eks.service;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.opentech.eks.Application;
import ru.opentech.eks.component.KafkaListener;
import ru.opentech.eks.component.KafkaListenerFactory;


@Service
public class TestListenService {

    private static final Logger log = LoggerFactory.getLogger( TestListenService.class );
    private KafkaListenerFactory listenerFactory;

    @Autowired
    public void setListenerFactory( KafkaListenerFactory kafkaListenerFactory ) {
        this.listenerFactory = kafkaListenerFactory;
    }

    public void start() {
        KafkaListener<Integer, Long> listener = new KafkaListener<Integer, Long>()
            .topics( Application.STREAM_NAME )
            .keyDeserializer( new IntegerDeserializer() )
            .valueDeserializer( new LongDeserializer() )
            .flatMap(
                record -> log.info( "key: {}, value: {}", record.key(), record.value() )
            );
        listenerFactory.listen( listener );
    }

}
