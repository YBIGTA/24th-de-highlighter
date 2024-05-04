package kafkastreams.example.KStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.StringTokenizer;

public class Producer {
    private final static Logger log = LoggerFactory.getLogger(Consumer.class);
    private final static String BOOTSTRAP_SERVERS = "43.202.65.95:9092";  /* change ip */
    private final static String TOPIC_NAME = "stream_filter";
    static BufferedReader br;
    static StringTokenizer str;

    public Producer() {}
    public static void main(String[] args) throws IOException {
        new Producer().run();
    }

    public void run() throws IOException {
        log.info("Starting Kafka producer...");
        KafkaProducer<String, String> producer = createKafkaProducer();

        // safe close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("closing producer...");
            producer.close();
            log.info("done!");
        }));

        log.info("Producer is ready");
        br = new BufferedReader(new InputStreamReader(System.in));
        boolean keyExist = false;

        while(true) {
            String key, value;
            if(keyExist) {
                str = new StringTokenizer(br.readLine());
                key = str.nextToken();
                value = str.nextToken();
            } else {
                key = null;
                value = br.readLine();
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

            if(value != null) {
                producer.send(record, (metadata, e) -> {
                    if (e != null) {
                        log.error("Something went wrong", e);
                    }
                });
            }
        }
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        System.out.println("BOOTSTRAP_SERVERS = " + BOOTSTRAP_SERVERS);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}

