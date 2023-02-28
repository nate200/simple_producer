package omg.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Map<String,Object> prop = new HashMap<>();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9094");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        ProducerRecord<String,String>[] records = new ProducerRecord[]{
                new ProducerRecord<>("test-simpler-producer","key1", "value1"),
                new ProducerRecord<>("test-simpler-producer","key2", "value2"),
                new ProducerRecord<>("test-simpler-producer","key3", "value3"),
                new ProducerRecord<>("test-simpler-producer","key4", "value4"),
        };

        for(ProducerRecord<String,String> record : records){
            System.out.println("sending a record: " + record.key() + ":" +record.value());
            producer.send(record);
        }
        producer.flush();
        producer.close();
    }
}