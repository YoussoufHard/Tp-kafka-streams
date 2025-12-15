package net.youssouf.clickproducerapp;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

@Controller
public class ClickController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private long clickCounter = 0;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/click")
    public String click() {
        // ✅ key = userId, value = numéro du clic
        long clickId = ++clickCounter;
        kafkaTemplate.send("clicks", "user1", String.valueOf(clickId));
        System.out.println("CLICK ENVOYÉ - ID: " + clickId);
        return "redirect:/";
    }
}
