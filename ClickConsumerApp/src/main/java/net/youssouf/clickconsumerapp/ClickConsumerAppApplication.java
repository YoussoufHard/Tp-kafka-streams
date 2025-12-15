package net.youssouf.clickconsumerapp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ClickConsumerAppApplication {

    private long clickCount = 0;

    public static void main(String[] args) {
        SpringApplication.run(ClickConsumerAppApplication.class, args);
    }

    @KafkaListener(topics = "click-counts", groupId = "click-consumer")
    public void listen(ConsumerRecord<String, String> record) {
        if (!"user1".equals(record.key())) {
            return;
        }

        // Compte tous les messages reçus, peu importe le contenu
        this.clickCount++;
        System.out.println("MESSAGE REÇU: [" + record.value() + "] | TOTAL MESSAGES: " + this.clickCount);
    }

    @GetMapping("/clicks/count")
    public long getClickCount() {
        return clickCount;
    }
}
