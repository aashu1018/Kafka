package kafka.wikimedia.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        //create Producer properties
        Properties properties = new Properties();

        //connect to Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "hello world");

        //send data
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    log.info("Received new metadata \n " +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition" + recordMetadata.partition() + "\n" +
                            "Timestamp" + recordMetadata.timestamp());
                }
            }
        });

        //tell the producer to send all data and block until its done
        producer.flush();

        //flush and close
        producer.close();

    }
}