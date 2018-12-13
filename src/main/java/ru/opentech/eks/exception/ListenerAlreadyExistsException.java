package ru.opentech.eks.exception;

import ru.opentech.eks.component.KafkaListener;

public class ListenerAlreadyExistsException extends RuntimeException {

    public ListenerAlreadyExistsException( KafkaListener listener ) {
        super( "Слушатель " + listener.toString() + " уже в работе." );
    }

}
