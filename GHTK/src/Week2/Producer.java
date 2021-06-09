package Week2;
import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("C:\\Users\\Hai\\Desktop\\haiht34\\emp.avsc"));
        String topic="haiht34_serializer2";
        GenericRecord e1=new GenericData.Record(schema);
        GenericData.Record ad = new GenericData.Record(schema.getField("address").schema());
        ad.put("pinCode", 10000);
        ad.put("streetName","Pham Hung");
        e1.put("ID",34);
        e1.put("email","ghtk@ghtk");
        e1.put("name","Hai");
        e1.put("address",ad);


        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
        datumWriter.write(e1,binaryEncoder);
        binaryEncoder.flush();
        out.close();
        byte[] serializedBytes = out.toByteArray();
        System.out.println(serializedBytes.toString());



        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"10.140.0.13:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> producer=new KafkaProducer<String,byte[]>(properties);

        ProducerRecord<String, byte[]> producerRecord= new ProducerRecord<String,byte[]>(topic,null,serializedBytes);
        producer.send(producerRecord);
        producer.close();

    }
}