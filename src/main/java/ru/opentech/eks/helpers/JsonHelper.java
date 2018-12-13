package ru.opentech.eks.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonHelper {

    private static final Logger log = LoggerFactory.getLogger( JsonHelper.class );
    private static final ObjectMapper mapper = new ObjectMapper();

    public static <V> V read( String value, Class<V> clazz ) {
        try {
            return mapper.readValue( value, clazz );
        }
        catch( IOException e ) {
            log.error( "Ошибка чтения json: {}", e.toString(), e );
            return null;
        }
    }

    private JsonHelper() {
        throw new UnsupportedOperationException( "Утилитный класс" );
    }

}
