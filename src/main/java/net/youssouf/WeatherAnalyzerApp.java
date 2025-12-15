package net.youssouf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Grouped;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WeatherAnalyzerApp {

    private static final Gauge avgTempGauge = Gauge.build()
            .name("weather_avg_temperature_fahrenheit")
            .help("Température moyenne par station")
            .labelNames("station")
            .register();

    private static final Gauge avgHumidityGauge = Gauge.build()
            .name("weather_avg_humidity")
            .help("Humidité moyenne par station")
            .labelNames("station")
            .register();

    private static final String INPUT_TOPIC = "weather-data";
    private static final String OUTPUT_TOPIC = "station-averages";

    public static void main(String[] args) throws IOException {

        createTopics();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analyzer-app-v3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 🔥 OBLIGATOIRE POUR TP
        props.put("auto.offset.reset", "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        KStream<String, WeatherData> parsed = source
                .mapValues(WeatherAnalyzerApp::parse)
                .filter((k, v) -> v != null && v.temperature > 30); // > 30°C

        KStream<String, WeatherData> fahrenheit = parsed
                .mapValues(v ->
                        new WeatherData(
                                v.station,
                                celsiusToFahrenheit(v.temperature),
                                v.humidity
                        )
                );

        KGroupedStream<String, WeatherData> grouped =
                fahrenheit.groupBy(
                        (k, v) -> v.station,
                        Grouped.with(Serdes.String(), new WeatherDataSerde())
                );

        KTable<String, StationAggregate> aggregated =
                grouped.aggregate(
                        StationAggregate::new,
                        (station, value, agg) -> {
                            agg.add(value.temperature, value.humidity);

                            // 🔥 METRICS PROMETHEUS
                            avgTempGauge.labels(station).set(agg.avgTemp());
                            avgHumidityGauge.labels(station).set(agg.avgHumidity());

                            return agg;
                        },
                        Materialized.with(Serdes.String(), new StationAggregateSerde())
                );

        aggregated
                .toStream()
                .mapValues(agg ->
                        String.format(
                                "Température Moyenne = %.1f°F, Humidité Moyenne = %.1f%%",
                                agg.avgTemp(),
                                agg.avgHumidity()
                        )
                )
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        DefaultExports.initialize(); // JVM metrics
        HTTPServer server = new HTTPServer(1234);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("⛔ Arrêt de l'application...");
            streams.close();
            server.stop();
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("🌦 WeatherAnalyzerApp démarrée...");
            latch.await(); // ⬅️ BLOQUE L’APP
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }

    // ================= UTILITAIRES =================

    private static WeatherData parse(String value) {
        try {
            String[] parts = value.split(",");
            return new WeatherData(
                    parts[0].trim(),
                    Double.parseDouble(parts[1].trim()),
                    Double.parseDouble(parts[2].trim())
            );
        } catch (Exception e) {
            return null;
        }
    }

    private static double celsiusToFahrenheit(double c) {
        return (c * 9 / 5) + 32;
    }

    private static void createTopics() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(
                    new NewTopic(INPUT_TOPIC, 1, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, 1, (short) 1)
            ));
        } catch (Exception ignored) {
        }
    }

    // ================= DATA CLASSES =================

    static class WeatherData {
        String station;
        double temperature;
        double humidity;

        WeatherData(String station, double temperature, double humidity) {
            this.station = station;
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }

    static class StationAggregate {
        double tempSum = 0;
        double humiditySum = 0;
        long count = 0;

        StationAggregate() {}

        StationAggregate(double tempSum, double humiditySum, long count) {
            this.tempSum = tempSum;
            this.humiditySum = humiditySum;
            this.count = count;
        }

        StationAggregate add(double temp, double hum) {
            tempSum += temp;
            humiditySum += hum;
            count++;
            return this;
        }

        double avgTemp() {
            return tempSum / count;
        }

        double avgHumidity() {
            return humiditySum / count;
        }
    }

    static class StationAggregateSerde implements Serde<StationAggregate> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Serializer<StationAggregate> serializer() {
            return new StationAggregateSerializer();
        }

        @Override
        public Deserializer<StationAggregate> deserializer() {
            return new StationAggregateDeserializer();
        }

        @Override
        public void close() {
        }
    }

    static class StationAggregateSerializer implements Serializer<StationAggregate> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, StationAggregate data) {
            if (data == null) return null;
            String serialized = data.tempSum + "," + data.humiditySum + "," + data.count;
            return serialized.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
        }
    }

    static class StationAggregateDeserializer implements Deserializer<StationAggregate> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public StationAggregate deserialize(String topic, byte[] data) {
            if (data == null) return null;
            String[] parts = new String(data, StandardCharsets.UTF_8).split(",");
            return new StationAggregate(
                    Double.parseDouble(parts[0]),
                    Double.parseDouble(parts[1]),
                    Long.parseLong(parts[2])
            );
        }

        @Override
        public void close() {
        }
    }

    static class WeatherDataSerde implements Serde<WeatherData> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public Serializer<WeatherData> serializer() {
            return new WeatherDataSerializer();
        }

        @Override
        public Deserializer<WeatherData> deserializer() {
            return new WeatherDataDeserializer();
        }

        @Override
        public void close() {
        }
    }

    static class WeatherDataSerializer implements Serializer<WeatherData> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, WeatherData data) {
            if (data == null) return null;
            String s = data.station + "," + data.temperature + "," + data.humidity;
            return s.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
        }
    }

    static class WeatherDataDeserializer implements Deserializer<WeatherData> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public WeatherData deserialize(String topic, byte[] bytes) {
            if (bytes == null) return null;
            String[] p = new String(bytes, StandardCharsets.UTF_8).split(",");
            return new WeatherData(
                    p[0],
                    Double.parseDouble(p[1]),
                    Double.parseDouble(p[2])
            );
        }

        @Override
        public void close() {
        }
    }
}
