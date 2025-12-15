package net.youssouf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@EnableKafkaStreams
@RestController
public class ClickCounterApp {

    private static final String CLICK_EVENTS_TOPIC = "click-events";
    private static final String CLICK_COUNTS_TOPIC = "click-counts";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public static void main(String[] args) {
        createKafkaTopics();
        SpringApplication.run(ClickCounterApp.class, args);
    }

    @Bean
    public KTable<String, Long> clickCounts(StreamsBuilder streamsBuilder) {
        KStream<String, String> clicks = streamsBuilder.stream(CLICK_EVENTS_TOPIC);

        return clicks
            .groupByKey()
            .count(Materialized.as("click-counts-store"));
    }

    @PostMapping("/click")
    public String sendClick() {
        kafkaTemplate.send(CLICK_EVENTS_TOPIC, "click", "1");
        return "Click sent!";
    }

    @GetMapping("/stats")
    public String getStats() {
        // For simplicity, return a message; in real app, query the store
        return "Total clicks: [query from store]";
    }

    private static void createKafkaTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (var admin = AdminClient.create(props)) {
            var newTopics = java.util.Arrays.asList(
                new NewTopic(CLICK_EVENTS_TOPIC, 1, (short) 1),
                new NewTopic(CLICK_COUNTS_TOPIC, 1, (short) 1)
            );

            admin.createTopics(newTopics).all().get();
            System.out.println("Topics créés avec succès");

        } catch (Exception e) {
            System.out.println("Les topics existent déjà ou erreur de création: " + e.getMessage());
        }
    }
}