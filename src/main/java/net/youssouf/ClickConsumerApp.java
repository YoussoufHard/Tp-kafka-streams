package net.youssouf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class ClickConsumerApp {

    private long clickCount = 0;

    public static void main(String[] args) {
        SpringApplication.run(ClickConsumerApp.class, args);
    }

    @KafkaListener(topics = "click-counts", groupId = "click-consumer")
    public void listen(ConsumerRecord<String, String> record) {
        if ("user1".equals(record.key())) {
            this.clickCount = Long.parseLong(record.value());
        }
    }

    @GetMapping("/clicks/count")
    public long getClickCount() {
        return clickCount;
    }
}