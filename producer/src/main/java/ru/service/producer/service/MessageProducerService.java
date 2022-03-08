package ru.service.producer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class MessageProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${kafka.topic.defaultName}")
    private String topicName;

    @Value(value = "${kafka.topic.partitionedName}")
    private String partitionedTopicName;

    @Value(value = "${kafka.topic.filteredName}")
    private String filteredTopicName;

    public MessageProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

        callBack(future, message);
    }

    public void sendMessageToPartition(String message, int partition) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(partitionedTopicName, partition, null, message);

        callBack(future, message);
    }

    public void sendMessageToFilter(String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(filteredTopicName, message);

        callBack(future, message);
    }

    private void callBack(ListenableFuture<SendResult<String, String>> future, String message) {
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Отправлено сообщение=[" + message + "], offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable e) {
                System.out.println("Не удалось отправить сообщение=[" + message + "] из-за ошибки : " + e.getMessage());
            }
        });
    }
}
