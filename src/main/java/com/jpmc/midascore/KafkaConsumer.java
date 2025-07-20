package com.jpmc.midascore;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public KafkaConsumer(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction message) {
        System.out.println("Transaction: " + message);
        
        UserRecord sender = userRepository.findById(message.getSenderId());

        // Validate Sender
        if (sender == null) {
            System.out.println("Sender not found: " + message.getSenderId());
            return;
        }

        UserRecord recipient = userRepository.findById(message.getRecipientId());
        
        // Validate Recipient
        if (recipient == null) {
            System.out.println("Recipient not found: " + message.getRecipientId());
            return;
        }

        // Validate sender has enough to cover transaction
        if (sender.getBalance() < message.getAmount()) {
            System.out.println("Insufficient balance for sender: " + message.getSenderId());
            return;
        }

        Incentive incentive = null;
        try {
            incentive = restTemplate.postForObject("http://localhost:8080/incentive", message, Incentive.class);
        } catch (Exception e) {
            System.out.println("Error fetching incentive: " + e.getMessage());
            return;
        }

        float incentiveAmount = incentive != null ? incentive.getAmount() : 0.0f;

        sender.setBalance(sender.getBalance() - message.getAmount());
        recipient.setBalance(recipient.getBalance() + message.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, message.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);

        System.out.println("Transaction processed successfully");
        System.out.println("Sender (" + sender.getName() + ") balance: " + sender.getBalance());
        System.out.println("Recipient (" + recipient.getName() + ") balance: " + recipient.getBalance());
        System.out.println("Transaction Record: " + transactionRecord);

        System.out.println(userRepository.findById(9).getBalance());
    }
}
