package ru.service.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MessageListenerService {
    @KafkaListener(topics = "${kafka.topic.defaultName}", groupId = "simpleGroup", containerFactory = "simpleKafkaListenerContainerFactory")
    public void listen(String message) {
        System.out.println("Получено сообщение из группы 'simpleGroup': " + message);
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topic.partitionedName}", partitions = {"0", "2"}), containerFactory = "partitionsKafkaListenerContainerFactory")
    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Получено сообщение: " + message + ", из партиции: " + partition);
    }

    @KafkaListener(topics = "${kafka.topic.filteredName}", containerFactory = "filterKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println("Получено отфильтрованное сообщение: " + message);
    }
}
