package net.youssouf.clickstreamsapp;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
@EnableKafkaStreams
public class ClickStreamsAppApplication {

	private static final String INPUT_TOPIC = "clicks";
	private static final String OUTPUT_TOPIC = "click-counts";

	public static void main(String[] args) {
		SpringApplication.run(ClickStreamsAppApplication.class, args);
	}

	@PostConstruct
	public void createTopics() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");

		try (AdminClient admin = AdminClient.create(props)) {
			admin.createTopics(List.of(
					new NewTopic(INPUT_TOPIC, 1, (short) 1),
					new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
			)).all().get();
		} catch (Exception ignored) {}
	}

	@Bean
	public KTable<String, Long> clickCounts(StreamsBuilder builder) {

		KStream<String, String> clicks =
				builder.stream(INPUT_TOPIC,
						Consumed.with(Serdes.String(), Serdes.String()));

		// Debug: voir les messages reçus
		clicks.peek((k, v) -> System.out.println("REÇU : " + k + " -> " + v));

		KTable<String, Long> counts = clicks
				.groupByKey()
				.count(Materialized.with(Serdes.String(), Serdes.Long()));

		// ✅ PRODUIT VERS Kafka - seulement les valeurs numériques valides
		counts.toStream()
				.filter((key, value) -> value != null && value >= 0) // Filtre valeurs nulles ou négatives
				.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

		return counts;
	}
}
