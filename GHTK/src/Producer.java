import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
public class Producer {
	public static void main(String[] args) {
		prod();
	}
	public static void prod() {
		//Tao properties
				String bootstrapServers="10.140.0.13:9092";
				Properties properties= new Properties();
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				//Tao producer
				KafkaProducer<String, String> first_producer = new KafkaProducer<String, String>(properties);
				//Tao producer record
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("test","aaaaaaaa");
				//Gui du lieu
				first_producer.send(record);
				first_producer.flush();
				first_producer.close();
	}
}
