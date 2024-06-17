package com.example.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerService {

    @Autowired
    private KafkaMessageProcessor kafkaMessageProcessor;

    @KafkaListener(topics = "TOPIC_A", groupId = "group_id")
    public void listenTopicA(String message) {
        kafkaMessageProcessor.processMessageFromTopicA(message);
    }

    @KafkaListener(topics = "TOPIC_B", groupId = "group_id")
    public void listenTopicB(String message) {
        kafkaMessageProcessor.processMessageFromTopicB(message);
    }
}
