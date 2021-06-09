package Week2;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

public class GenericRecordProducer {

    public static void main(String[] args) throws IOException {
        GenericRecordProducer genericRecordProducer = new GenericRecordProducer();
        genericRecordProducer.writeMessage();
    }

    public void writeMessage() throws IOException {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://10.140.0.13:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        //avro schema
        Schema schema = new Schema.Parser().parse(new File("C:\\Users\\Hai\\Desktop\\haiht34\\emp.avsc"));

      //Instantiating the GenericRecord class.
        GenericRecord e1 = new GenericData.Record(schema);
  		GenericRecord add = new GenericData.Record(schema.getField("address").schema());
        //Insert data according to schema
  		add.put("pinCode",10000);
  		add.put("streetName","Pham Hung");
        e1.put("ID", 007);
        e1.put("address", add);
        e1.put("email","ghtk@ghtk");
        e1.put("name", "haiht34");

        //prepare the kafka record
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("haiht34_serializer", null, e1);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();
    }
}