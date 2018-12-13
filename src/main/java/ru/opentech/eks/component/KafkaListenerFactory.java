package ru.opentech.eks.component;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.opentech.eks.exception.ListenerAlreadyExistsException;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class KafkaListenerFactory {

    private final ScheduledExecutorService kafkaListenerThreadPool = Executors.newScheduledThreadPool( 5 );
    private final Map<KafkaListener, KafkaListenerTask> listenerPool = new ConcurrentHashMap<>();

    private final Properties properties;

    public KafkaListenerFactory(
        String bootstrapServers,
        String groupId,
        boolean autoCommit,
        int autoCommitIntervalMs
    ) {
        this.properties = new Properties();
        this.properties.put( "bootstrap.servers", bootstrapServers );
        this.properties.put( "group.id", groupId );
        this.properties.put( "enable.auto.commit", autoCommit );
        this.properties.put( "auto.commit.interval.ms", autoCommitIntervalMs );
        this.properties.put( "auto.offset.reset", "earliest" );
    }

    public <K, V> void listen( KafkaListener<K, V> listener ) {
        if( kafkaListenerThreadPool.isShutdown() ) {
            throw new IllegalStateException( "Слушатели выключены!" );
        }
        if( listenerPool.containsKey( listener ) ) {
            throw new ListenerAlreadyExistsException( listener );
        }
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(
            properties, listener.getKeyDeserializer(), listener.getValueDeserializer()
        );
        consumer.subscribe( listener.getTopics() );
        ScheduledFuture future = kafkaListenerThreadPool.scheduleAtFixedRate(
            () -> consumer.poll( listener.getPollTimeout() ).forEach( listener.getHandler()::apply ),
            0, 500, TimeUnit.MILLISECONDS
        );
        listenerPool.put( listener, new KafkaListenerTask( consumer, future ) );
    }

    public void unlisten( KafkaListener listener ) {
        KafkaListenerTask task = listenerPool.get( listener );
        if( task != null ) {
            task.scheduledFuture.cancel( true );
            task.kafkaConsumer.close();
            listenerPool.remove( listener );
        }
    }

    @PreDestroy
    public void shutdown() {
        listenerPool.keySet().forEach( this::unlisten );
        kafkaListenerThreadPool.shutdownNow();
    }

    private static class KafkaListenerTask {

        private final KafkaConsumer kafkaConsumer;
        private final ScheduledFuture scheduledFuture;

        private KafkaListenerTask( KafkaConsumer kafkaConsumer, ScheduledFuture scheduledFuture ) {
            this.kafkaConsumer = kafkaConsumer;
            this.scheduledFuture = scheduledFuture;
        }

    }

}
