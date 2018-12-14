package ru.opentech.eks.component;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.opentech.eks.exception.StreamAlreadyExistsException;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KafkaStreamFactory {

    private static final Logger log = LoggerFactory.getLogger( KafkaStreamFactory.class );
    private final ExecutorService kafkaStreamThreadPool = Executors.newCachedThreadPool();
    private final Map<Topology, KafkaStreamTask> streamPool = new ConcurrentHashMap<>();

    private final Properties properties;

    public KafkaStreamFactory( String bootstrapServers ) {
        this.properties = new Properties();
        this.properties.put( StreamsConfig.APPLICATION_ID_CONFIG, "test" );
        this.properties.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        this.properties.put( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0 );
        this.properties.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );
    }

    public KafkaStreams startStream( Topology topology ) {
        if( kafkaStreamThreadPool.isShutdown() ) {
            throw new IllegalStateException( "Потоки выключены!" );
        }
        if( streamPool.containsKey( topology ) ) {
            throw new StreamAlreadyExistsException( topology );
        }
        log.info( "Запуск потока {}", topology.describe() );
        KafkaStreams streams = new KafkaStreams( topology, properties );
        streams.cleanUp();
        Future future = kafkaStreamThreadPool.submit( streams::start );
        streamPool.put( topology, new KafkaStreamTask( streams, future ) );
        return streams;
    }

    public void stopStream( Topology topology ) {
        KafkaStreamTask task = streamPool.get( topology );
        if( task != null ) {
            task.future.cancel( true );
            task.stream.close();
            streamPool.remove( topology );
        }
    }

    @PreDestroy
    public void shutdown() {
        streamPool.keySet().forEach( this::stopStream );
        kafkaStreamThreadPool.shutdownNow();
    }

    private static class KafkaStreamTask {

        private final KafkaStreams stream;
        private final Future future;

        private KafkaStreamTask( KafkaStreams stream, Future future ) {
            this.stream = stream;
            this.future = future;
        }

    }

}
