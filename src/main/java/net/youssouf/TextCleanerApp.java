package net.youssouf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TextCleanerApp {
    
    private static final String INPUT_TOPIC = "text-input";
    private static final String CLEAN_TOPIC = "text-clean";
    private static final String DEAD_LETTER_TOPIC = "text-dead-letter";
    private static final String[] FORBIDDEN_WORDS = {"HACK", "SPAM", "XXX"};
    private static final int MAX_MESSAGE_LENGTH = 100;

    public static void main(String[] args) {
        try {
            // Création des topics Kafka
            createKafkaTopics();
            
            // Configuration de Kafka Streams
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            
            // Construction de la topologie
            StreamsBuilder builder = new StreamsBuilder();
            
            // Lecture du topic d'entrée
            KStream<String, String> textStream = builder.stream(INPUT_TOPIC);
            
            // Traitement du texte
            KStream<String, String> cleaned = textStream.mapValues(TextCleanerApp::cleanText);

            cleaned.filter((k,v) -> isValidMessage(v))
                   .to(CLEAN_TOPIC);

            cleaned.filter((k,v) -> !isValidMessage(v))
                   .to(DEAD_LETTER_TOPIC);
            
            // Démarrage de l'application
            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            
            // Gestion de l'arrêt
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            
            System.out.println("Démarrage de l'application...");
            streams.start();
            
            // Attente de l'arrêt
            Thread.currentThread().join();
            
        } catch (Exception e) {
            System.err.println("Erreur : " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static String cleanText(String text) {
        if (text == null) return "";
        return text.trim().replaceAll("\\s+", " ").toUpperCase();
    }
    
    private static boolean isValidMessage(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }
        
        // Vérification des mots interdits
        for (String word : FORBIDDEN_WORDS) {
            if (text.contains(word)) {
                return false;
            }
        }
        
        // Vérification de la longueur
        return text.length() <= MAX_MESSAGE_LENGTH;
    }
    
    private static void createKafkaTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        
        try (var admin = AdminClient.create(props)) {
            // Création des topics s'ils n'existent pas
            var newTopics = java.util.Arrays.asList(
                new NewTopic(INPUT_TOPIC, 1, (short) 1),
                new NewTopic(CLEAN_TOPIC, 1, (short) 1),
                new NewTopic(DEAD_LETTER_TOPIC, 1, (short) 1)
            );
            
            admin.createTopics(newTopics).all().get();
            System.out.println("Topics créés avec succès");
            
        } catch (Exception e) {
            System.out.println("Les topics existent déjà ou erreur de création: " + e.getMessage());
        }
    }
}