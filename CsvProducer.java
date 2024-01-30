import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class CsvProducer {

    public static void main(String[] args) {
        // Set up Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Specify the Kafka topic to which messages will be sent
        String topic1 = "csv-topic1";
        String topic2 = "csv-topic2";
        String topic3 = "csv-topic3";

        // Send data from three CSV files to Kafka topic
        sendCsvDataToKafka("src/main/resources/username1.csv", topic1, producer);
        sendCsvDataToKafka("src/main/resources/username2.csv", topic2, producer);
        sendCsvDataToKafka("src/main/resources/username3.csv", topic3, producer);

        // Close the Kafka producer
        producer.close();
    }

    private static void sendCsvDataToKafka(String csvFileName, String topic, Producer<String, String> producer) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Assuming each line in the CSV file represents a message
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
                producer.send(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

