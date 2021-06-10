package Week2;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("C:\\Users\\Hai\\eclipse-workspace\\GHTK\\src\\Week2\\emp.avsc"));


        Properties properties =new Properties();
        properties.put("bootstrap.servers","10.140.0.13:9092");
        properties.put("group.id","haiht34_v12");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Schema.Parser parser = new Schema.Parser();


        KafkaConsumer<String,byte[]> consumers=new KafkaConsumer<String, byte[]>(properties);
        consumers.subscribe(Arrays.asList("haiht34_serializer2"));


        while (true) {
            ConsumerRecords<String, byte[]> records = consumers.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, byte[]> record : records) {
                byte[] data=record.value();
                SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                GenericRecord user = reader.read(null,decoder);
                System.out.println(user);
            }
        }

    }

}
