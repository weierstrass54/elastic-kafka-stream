package ru.opentech.eks;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.opentech.eks.service.TestListenService;
import ru.opentech.eks.service.TestStreamService;

@SpringBootApplication
public class Application implements CommandLineRunner {

    public static final String STREAM_NAME = "catalog-test-count";

    private TestListenService testListenService;
    private TestStreamService testStreamService;

    @Autowired
    public void setTestListenService( TestListenService testListenService ) {
        this.testListenService = testListenService;
    }

    @Autowired
    public void setTestStreamService( TestStreamService testStreamService ) {
        this.testStreamService = testStreamService;
    }

    public static void main(String[] args ) {
        SpringApplication.run( Application.class, args );
    }

    @Override
    public void run( String... args ) {
        testListenService.start();
        //testStreamService.start();
    }

}
