package ampw.store.read;

import ampw.store.read.KafkaConsumer.KafkaStreamsConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ampw.store.read.KafkaConsumer.ConsumerProperties.properties;

//@SpringBootApplication
public class StoreReadApplication {

    public static void main(String[] args) {
        int numConsumers = 1;
//        SpringApplication.run(StoreReadApplication.class, args);

        List<String> topics = Arrays.asList("readFromProducts");

        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<KafkaStreamsConsumer> consumers = new ArrayList<>();
            KafkaStreamsConsumer consumer = new KafkaStreamsConsumer(properties(), topics, numConsumers);
            consumers.add(consumer);
            executor.submit(consumer);

            executor.shutdown();

//        registerShutdownHook(executor, consumers);
    }
}