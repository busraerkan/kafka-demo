package com.busraerkan.kafkademo.producer;

import com.busraerkan.kafkademo.constant.Constant;
import com.busraerkan.kafkademo.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/produce")
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/message")
    public ResponseEntity<?> sendMessage(@RequestBody User message) {
        try {
            kafkaTemplate.send(Constant.TOPIC_NAME, message);
        } catch (Exception e) {
            log.error("Exception occured while producing message : ", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
