package com.busraerkan.kafkademo.consumer;

import com.busraerkan.kafkademo.constant.Constant;
import com.busraerkan.kafkademo.model.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {
    @KafkaListener(groupId = Constant.GROUP_ID_JSON, topics = Constant.TOPIC_NAME,
            containerFactory = Constant.KAFKA_LISTENER_CONTAINER_FACTORY)
    public void receivedMessage(User message) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(message);
        log.info("Json message received " + jsonString);
    }
}
