import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Consumer;

public class CountWord {
    public static Properties  ConfigProperty(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.140.0.13:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"haiht34_v8");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }
    public static void ProducerFunction(String path,String topicName,Properties properties) throws IOException {
        File file = new File(path);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String readFileByLine;
        while ((readFileByLine = bufferedReader.readLine()) != null){
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,readFileByLine);
            producer.send(record);
        }
        System.out.println("Post data successfully");
    }
    public static void main(String []args) throws IOException {
        final String topicName = "haiht34_bai4";

        // set config for properties producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers","10.140.0.13:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ProducerFunction("C:\\Users\\Hai\\Desktop\\data.txt",topicName,properties);



        //set configu for consumer
        Properties propertiesConsumer = new Properties();
        propertiesConsumer.put("bootstrap.servers","10.140.0.13:9092");
        propertiesConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        propertiesConsumer.put(ConsumerConfig.GROUP_ID_CONFIG,"haiht34_v8");
        propertiesConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        String []wordList = {"windmill" , "Don", "Quixote", "Manuel", "combat", "man", "woman", "lady", "mountain", "sword"};
        // print wordlist
        ConsumerFunction(topicName, propertiesConsumer,wordList);







    }

    private static int Count(String word, String str) { // count 1 word in pagagraph
    	str = StringUtils.lowerCase(str);
		int count = StringUtils.countMatches(str, word);
        return count;
    }

    public static void ConsumerFunction(String topicName,Properties properties,String[] Words) {
        KafkaConsumer<String,String> consumers = new KafkaConsumer<String, String>(properties);
        consumers.subscribe(Arrays.asList(topicName));
        String data = "";
        int size = Words.length; // init array to count index wordLIst
        int []count = new int[size];
        for(int a = 0 ; a < size ; a++){
            count[a] = 0;
        }
        while (true) {

            ConsumerRecords<String, String> records = consumers.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                data = record.value().toLowerCase();
                String mess = data.toString();
                for(int a = 0 ; a < size ; a++) {
                    count[a] +=  Count(Words[a].toLowerCase(), mess);
                    System.out.println(Words[a] +": "+" : "+  count[a]);
                }
                data = "";
            }
        }



    }
}
