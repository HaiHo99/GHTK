package Week2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Consumer;

public class ProducerAvro {
    public static void ProducerFunction(String topicName,Properties properties) throws IOException {
        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);
      //avro schema
        Schema schema = new Schema.Parser().parse(new File("C:\\Users\\Hai\\Desktop\\haiht34\\emp.avsc"));
      //Instantiating the GenericRecord class.
        GenericRecord e1 = new GenericData.Record(schema);
  		
        //Insert data according to schema
        e1.put("name", "ramu");
        e1.put("id", 001);
        e1.put("salary",30000);
        e1.put("age", 25);
        e1.put("address", "chenni");
        
    	ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("haiht_serializer", null, e1);
    	producer.send(record);
        System.out.println("Post data successfully");
    }
    public static void main(String []args) throws IOException {
        final String topicName = "haiht34_serializer";

        // set config for properties producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        ProducerFunction(topicName,properties);
    }
}
