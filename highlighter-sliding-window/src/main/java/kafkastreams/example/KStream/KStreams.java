package kafkastreams.example.KStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class KStreams {
    private static final String BOOTSTRAP_SERVERS = "3.35.19.251:9092"; // IP 바꾸세요
    private static final String APPLICATION_NAME = "timestamp-count-application";
    private static final String STREAM_SOURCE = "bab";  // Source 바꾸세요
    private static final String STREAM_SINK = "bab_sink";   // Sink 바꾸세요
    private static final long THRESHOLD = 10;  // Example threshold

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
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(1)))
                .count();

        KStream<String, String> outputStream = timestampCounts
                .toStream()
                .transform(() -> new ThresholdExceedTransformer(), Named.as("ThresholdExceed"));

        outputStream.to(STREAM_SINK, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static class ThresholdExceedTransformer implements Transformer<Windowed<Long>, Long, KeyValue<String, String>> {
        private boolean thresholdExceeded = false;
        private long startTime = 0;

        @Override
        public void init(ProcessorContext context) {
        }

        @Override
        public KeyValue<String, String> transform(Windowed<Long> key, Long value) {
            if (value > THRESHOLD) {
                if (!thresholdExceeded) {
                    thresholdExceeded = true;
                    startTime = key.window().start();
                }
            } else if (thresholdExceeded) {
                thresholdExceeded = false;
                long endTime = key.window().end();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                // 시작 시간과 종료 시간을 사용자 친화적인 형식으로 포맷팅합니다.
                String start = Instant.ofEpochMilli(startTime).atZone(ZoneId.systemDefault()).format(formatter);
                String end = Instant.ofEpochMilli(endTime).atZone(ZoneId.systemDefault()).format(formatter);

                // 결과를 KeyValue 객체로 반환합니다.
                return new KeyValue<>(start, "Threshold exceeded from " + start + " to " + end);
            }
            return null;
        }

        @Override
        public void close() {
        }
    }
}
