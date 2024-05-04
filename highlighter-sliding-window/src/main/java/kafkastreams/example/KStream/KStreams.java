package kafkastreams.example.KStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsConfig;
import java.time.Duration;
import java.util.Properties;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import org.apache.kafka.streams.KeyValue;


public class KStreams {

    private final static String BOOTSTRAP_SERVERS = "43.202.65.95:9092";  /* change ip */
    private final static String APPLICATION_NAME = "timestamp-count-application";
    private final static String STREAM_SOURCE = "stream_filter";
    private final static String STREAM_SINK = "stream_filter_sink";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put("default.value.serde", Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_SOURCE);

        // Define a DateTimeFormatter for the expected timestamp format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Parse timestamps and convert to epoch milliseconds
        KStream<String, Long> timestampsToEpoch = streamLog
                .filter((key, value) -> value.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) // Ensure format
                .mapValues(value -> {
                    return LocalDateTime.parse(value, formatter)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
                });

        // Group by the epoch milliseconds and count entries
        KTable<Windowed<Long>, Long> timestampCounts = timestampsToEpoch
                .groupBy((key, value) -> value, Grouped.with(Serdes.Long(), Serdes.Long()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5))) // 5-minute windows
                .count();

        // Convert the KTable back to KStream to write back to Kafka
        KStream<String, Long> outputStream = timestampCounts
                .toStream((windowedKey, value) -> String.valueOf(KeyValue.pair(windowedKey.key().toString(), value)));

        outputStream.to(STREAM_SINK, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

