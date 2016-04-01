package skyler.tao.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.serializer.StringEncoder;

public class SimpleProducer {

	public static void main(String[] args) {
		Properties prop = new Properties();
//		prop.put("metadata.broker.list", "172.16.89.128:9092");
		prop.put("bootstrap.servers", "172.16.89.128:9092,172.16.89.129:9092,172.16.89.130:9092");
		prop.put("acks", "all");		//not request.required.acks
//		prop.put("serializer.class", StringSerializer.class.getName());		//unknown config
		prop.put("key.serializer", StringSerializer.class.getName());
		prop.put("value.serializer", StringSerializer.class.getName());
		
		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(prop);
		
		for (int iCount = 0; iCount < 10; iCount++) {
			String message = "My Test Message No " + iCount;
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("test-topic-tao", message);
			producer.send(record);
		}
		producer.close();
	}
}
