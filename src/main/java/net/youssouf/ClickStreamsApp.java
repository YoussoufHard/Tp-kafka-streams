package net.youssouf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ClickStreamsApp {

    private static final String INPUT_TOPIC = "clicks";
    private static final String OUTPUT_TOPIC = "click-counts";

    public static void main(String[] args) {
        try {
            // Création des topics Kafka
            createKafkaTopics();

            // Configuration de Kafka Streams
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-streams-app");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            // Construction de la topologie
            StreamsBuilder builder = new StreamsBuilder();

            // Lecture du topic d'entrée
            KStream<String, String> clickStream = builder.stream(INPUT_TOPIC);

            // Comptage des clics par utilisateur
            KTable<String, Long> clickCounts = clickStream
                .groupByKey()
                .count(Materialized.as("click-counts-store"));

            // Publication des résultats
            clickCounts.toStream().to(OUTPUT_TOPIC);

            // Démarrage de l'application
            KafkaStreams streams = new KafkaStreams(builder.build(), props);

            // Gestion de l'arrêt
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

            System.out.println("Démarrage de l'application Click Streams...");
            streams.start();

            // Attente de l'arrêt
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("Erreur : " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void createKafkaTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (var admin = AdminClient.create(props)) {
            // Création des topics s'ils n'existent pas
            var newTopics = java.util.Arrays.asList(
                new NewTopic(INPUT_TOPIC, 1, (short) 1),
                new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
            );

            admin.createTopics(newTopics).all().get();
            System.out.println("Topics créés avec succès");

        } catch (Exception e) {
            System.out.println("Les topics existent déjà ou erreur de création: " + e.getMessage());
        }
    }
}