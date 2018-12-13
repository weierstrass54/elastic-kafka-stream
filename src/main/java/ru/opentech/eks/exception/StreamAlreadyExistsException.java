package ru.opentech.eks.exception;

import org.apache.kafka.streams.Topology;

public class StreamAlreadyExistsException extends RuntimeException {

    public StreamAlreadyExistsException( Topology topology ) {
        super( "Поток " + topology.toString() + " уже в работе." );
    }

}
