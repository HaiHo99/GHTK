package Week2;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class GenericRecordConsumer {

    public static void main(String[] args) {
        GenericRecordConsumer genericRecordConsumer = new GenericRecordConsumer();
        genericRecordConsumer.readMessages();
    }

    public void readMessages() {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.13:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "generic-record-cons");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.13:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(properties);

        consumer.subscribe(Arrays.asList("haiht34_serializer"));

        //poll the record from the topic
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, GenericRecord> record : records) {
              System.out.println(record.value());
            }
            consumer.commitAsync();
        }

    }
}