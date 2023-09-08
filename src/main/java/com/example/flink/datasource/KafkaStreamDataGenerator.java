package com.example.flink.datasource;

import com.example.flink.model.BrowserSessionEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class KafkaStreamDataGenerator  implements Runnable{
    private static final String KAFKA_BROKER = "localhost:19092"; // Replace with your Kafka broker address
    private static final String TOPIC_NAME = "browser-session-events"; // Kafka topic name

    public static void main(String[] args) {
        KafkaStreamDataGenerator fsdg = new KafkaStreamDataGenerator();
        fsdg.run();
    }

    private static String serializeEvent(BrowserSessionEvent event) {
        // Serialize BrowserSessionEvent as a CSV string
        return String.format("%s,%s,%d", event.getUserName(), event.getAction(), event.getTimestamp());
    }

        @Override
        public void run() {
            try {
                // Configure Kafka producer properties

                Properties kafkaProps = new Properties();
            kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            kafkaProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // Create a Kafka producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

            // Generate and send browser session events to Kafka in CSV format

                generateAndSendEvents(producer);
                producer.close();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // Close the Kafka producer
        }

        private static void generateAndSendEvents(KafkaProducer<String, String> producer) throws InterruptedException {
            Random random = new Random();
            String[] users = {"User1", "User2", "User3", "User4", "User5"};
            String[] actions = {"login", "logout", "view video", "view link", "view review"};

            try {
                while (true) {
                    String user = users[random.nextInt(users.length)];
                    String action = actions[random.nextInt(actions.length)];
                    long timestamp = System.currentTimeMillis();

                    // Create a browser session event
                    BrowserSessionEvent event = new BrowserSessionEvent(user, action, timestamp);

                    // Serialize the event as a CSV string and send it to Kafka
                    String eventCsv = serializeEvent(event);
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, eventCsv);
                    producer.send(record);

                    // Sleep for a random period (adjust as needed)
                    Thread.sleep(random.nextInt(1000) + 500); // Sleep between 500ms and 1500ms
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

}
