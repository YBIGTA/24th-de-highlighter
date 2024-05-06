package kafkastreams.example.KStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsConfig;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Properties;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import org.apache.kafka.streams.KeyValue;


public class KStreams {

    private final static String BOOTSTRAP_SERVERS = "43.201.57.179:9092";  /* change ip */
    private final static String APPLICATION_NAME = "timestamp-count-application";
    private final static String STREAM_SOURCE = "stream_filter";
    private final static String STREAM_SINK = "stream_filter_sink";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_SOURCE);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        KStream<String, Long> timestampsToEpoch = streamLog
                .filter((key, value) -> value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"))
                .mapValues(value -> {
                    try {
                        return LocalDateTime.parse(value, formatter)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                    } catch (Exception e) {
                        System.err.println("Failed to parse date: " + value);
                        return null;
                    }
                });

        KTable<Windowed<Long>, Long> timestampCounts = timestampsToEpoch
                .filter((key, value) -> value != null)
                .groupBy((key, value) -> value, Grouped.with(Serdes.Long(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .count();

        KStream<String, String> outputStream = timestampCounts
                .toStream()
                .map((key, value) -> {
                    // Convert the millisecond timestamp back to a date string
                    String timestampStr = Instant.ofEpochMilli(key.window().start())
                            .atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    return KeyValue.pair(timestampStr, timestampStr + " , " + value);
                });

        outputStream.peek((key, value) -> System.out.println("Sending to Kafka: Key = " + key + ", Value = " + value));
        outputStream.to(STREAM_SINK, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

