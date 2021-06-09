import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
public class Consumer {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
		String bootstrapServers = "10.140.0.13:9092";
		String grp_id="second_app";
		String topic="test";
		//Tao consumer properties
		Properties properties= new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		//Tao consumer
		KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);
		//Subcribe
		consumer.subscribe(Arrays.asList(topic));
		//Polling
		while(true) {
			ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: records) {
				logger.info("Key: "+ record.key() + ", Value: "+ record.value());
				logger.info("Partition:" + record.partition()+"Offset:"+record.offset());
			}
		}
	}
}
