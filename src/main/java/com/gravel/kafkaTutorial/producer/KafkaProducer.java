package com.gravel.kafkaTutorial.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * @author Gravel
 * @date 2019/10/11.
 */
@Component
@Slf4j
public class KafkaProducer implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        log.info("start producer 10 message ");
        for (int i = 0; i < 10; i++) {
            kafkaTemplate.send("showcase-1", UUID.randomUUID().toString(), "i" + i);
            kafkaTemplate.send("showcase-2", UUID.randomUUID().toString(), "i" + i);
            kafkaTemplate.send("showcase-3", UUID.randomUUID().toString(), "i" + i);

        }
    }
}
