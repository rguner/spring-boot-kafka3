package com.guner.kafka.service;

import com.guner.kafka.model.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.net.SocketTimeoutException;

@Service
@Slf4j
public class KafkaReceiver {

    @KafkaListener(topics = "topic-1", groupId = "group-1")
    public void listenGroupGroup1(String message) {
        log.info("-----   Received Message in group group-1 {}", message);
    }

    @RetryableTopic(
            backoff = @Backoff(value = 3000L),
            attempts = "5",
            //autoCreateTopics = "false", it creates even it is false
            include = RuntimeException.class)
    @KafkaListener(topics = "topic-1", groupId = "group-2")
    public void listenAndRetryIfRequired(String message) {
        log.info("-----   Received Message in group group-2: {}", message);
        throw new RuntimeException("Receive Exception to test retry mechanism");
    }


    //@KafkaListener(topics = "topic-1, topic-2", groupId = "group-1")
    @KafkaListener(topics = "topic-2", groupId = "group-1")
    public void listenMultipleTopicsGroupGroup1(String message) {
        log.info("-----   Received Message on topic-2 in group group-1: {}", message);
    }

}
