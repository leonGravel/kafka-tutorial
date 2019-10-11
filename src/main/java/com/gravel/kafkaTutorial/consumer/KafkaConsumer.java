package com.gravel.kafkaTutorial.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author Gravel
 * @date 2019/9/26.
 */
@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(id = "tut1", topicPattern = "showcase.*")
    public void listen(ConsumerRecord<?, ?> record, Acknowledgment ack) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("Receive： +++++++++++++++ partition:" + record.partition());
            log.info("Receive： +++++++++++++++ Topic:" + record.topic());
            log.info("Receive： +++++++++++++++ Record:" + record);
            log.info("Receive： +++++++++++++++ Message:" + message);
            ack.acknowledge();
        }
    }

}
