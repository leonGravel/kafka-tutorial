package com.gravel.kafkaTutorial.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
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
    public void listen(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("Receive： +++++++++++++++ partition:" + record.partition());
            log.info("Receive： +++++++++++++++ Topic:" + topic);
            log.info("Receive： +++++++++++++++ Record:" + record);
            log.info("Receive： +++++++++++++++ Message:" + message);
        }
    }

    @KafkaListener(id = "tut2", topicPattern = "showcase.*")
    public void tut2(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            log.info("Receive： +tut2++++++++++++++ partition:" + record.partition());
            log.info("Receive： ++tut2+++++++++++++ Topic:" + topic);
            log.info("Receive： +tut2++++++++++++++ Record:" + record);
            log.info("Receive： +tut2++++++++++++++ Message:" + message);
        }
    }

//    @KafkaListener(id = "tut1",group = "group1",topics = "test1")
//    public void group1(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
//        if (kafkaMessage.isPresent()) {
//            Object message = kafkaMessage.get();
//            log.info("Receive： +group1++++++++++++++ Topic:" + topic);
//            log.info("Receive： +group1++++++++++++++ Record:" + record);
//            log.info("Receive： +group1++++++++++++++ Message:" + message);
//        }
//    }

//    @KafkaListener(group = "group2",topics = "test1")
//    public void group2(ConsumerRecord<?, ?> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
//        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
//        if (kafkaMessage.isPresent()) {
//            Object message = kafkaMessage.get();
//            log.info("Receive： +group2++++++++++++++ Topic:" + topic);
//            log.info("Receive： +group2++++++++++++++ Record:" + record);
//            log.info("Receive： +group2++++++++++++++ Message:" + message);
//        }
//    }
}
