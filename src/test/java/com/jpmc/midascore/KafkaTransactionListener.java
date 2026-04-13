package com.jpmc.midascore;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    @Transactional
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        String senderLabel = sender != null ? sender.getName() : "(unknown id " + transaction.getSenderId() + ")";
        String recipientLabel = recipient != null ? recipient.getName() : "(unknown id " + transaction.getRecipientId() + ")";
        System.out.println("Received: " + transaction + " | " + senderLabel + " -> " + recipientLabel);

        if (sender == null || recipient == null) {
            System.out.println("  discarded (invalid senderId or recipientId)");
            return;
        }

        float amount = transaction.getAmount();
        if (sender.getBalance() < amount) {
            System.out.println("  discarded (insufficient balance)");
            return;
        }

        Incentive incentiveResponse = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = incentiveResponse != null ? incentiveResponse.getAmount() : 0.0f;

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = transactionRecordRepository.save(
                new TransactionRecord(sender, recipient, amount, incentiveAmount)
        );

        System.out.println("  recorded " + record);
    }
}