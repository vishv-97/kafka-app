import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.PrintStream;
import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {
        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Specify the Kafka topic to which messages will be sent
        String topic = "vishv-test";

        // Send one million string messages
        int numMessages = 100;
        int initialValue =1;
        for (int i = 1; i <= numMessages; i++) {
            int valueToAdd = 2 * i;
            String result = "Message: " + initialValue + valueToAdd;
            //String message = "Message " + i;

            // Create a ProducerRecord with the topic, key, and value
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, result);

            // Send the record to the Kafka topic
            producer.send(record);
        }

        int num = 5;
        for (int i = 1; i <= 10; ++i) {
            String message = String.format("%d * %d = %d", num, i, num * i);
            //String message = "Message " + i;

            // Create a ProducerRecord with the topic, key, and value
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);

            // Send the record to the Kafka topic
            producer.send(record);
        }

        // Close the Kafka producer
        producer.close();
    }
}
