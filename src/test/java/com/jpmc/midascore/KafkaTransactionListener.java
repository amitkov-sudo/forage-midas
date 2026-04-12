package com.jpmc.midascore;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.kafka.annotation.KafkaListener;

@Component
public class KafkaTransactionListener {
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction){
        System.out.println("Received: " + transaction);
    }
}
