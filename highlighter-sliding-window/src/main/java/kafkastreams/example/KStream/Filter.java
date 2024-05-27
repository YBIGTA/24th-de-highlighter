package kafkastreams.example.KStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Filter {

    private final static String BOOTSTRAP_SERVERS = "43.203.141.74:9092";  /* change ip */
    private final static String APPLICATION_NAME = "word-filter-application";
    private final static String STREAM_SOURCE = "stream_filter";
    private final static String STREAM_SINK = "stream_filter_sink";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 핵심부
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_SOURCE);
        KStream<String, String> filterStream = streamLog.filter(
                (key, value) -> value.length() > 5);
        filterStream.to(STREAM_SINK);

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }
}

