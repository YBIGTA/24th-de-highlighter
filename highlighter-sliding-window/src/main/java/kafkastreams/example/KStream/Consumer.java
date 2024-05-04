package kafkastreams.example.KStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class);
    private final static String BOOTSTRAP_SERVERS = "43.202.65.95:9092";  /* change ip */
    private final static String GROUP_ID = "kstream-application";  /* this can be anything you want */
    private final static String TOPIC_NAME = "stream_filter_sink";

    public Consumer() {}
    public static void main(String[] args) {
        new Consumer().run();
    }

    public void run() {
        log.info("Starting Kafka consumer...");
        KafkaConsumer<String, Long> consumer = createKafkaConsumer();

        // safe close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("closing consumer...");
            consumer.close();
            log.info("done!");
        }));
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        log.info("Consumer is ready");
        try {
            while(true) {
                ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Long> record : records) {
                    log.info("Timestamp: " + record.key() + ", Count: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (Exception e) {
            log.error("Error in consuming records", e);
        } finally {
            consumer.close();
            log.info("Consumer closed");
        }
    }

    public KafkaConsumer<String, Long> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }
}