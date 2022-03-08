package ru.service.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.service.producer.service.MessageProducerService;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(ProducerApplication.class, args);

        MessageProducerService producer = context.getBean(MessageProducerService.class);

        // simple
        for (int i = 0; i < 10; i++) {
            producer.sendMessage("Simple message " + i);

            TimeUnit.SECONDS.sleep(1);
        }

        // partitioned
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartition("Partitioned message", i);

            TimeUnit.SECONDS.sleep(1);
        }

        // filtered
        producer.sendMessageToFilter("Hello Test");
        producer.sendMessageToFilter("Hello World");

        context.close();
    }

}
