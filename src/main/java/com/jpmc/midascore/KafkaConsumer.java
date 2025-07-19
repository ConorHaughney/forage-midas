package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction message) {
        System.out.println("Transaction: " + message);
        System.out.println("----------------------------------------------------------");
        System.out.println("----------------------------------------------------------");
    }
}
