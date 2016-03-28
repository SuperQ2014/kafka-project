package skyler.tao.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

	public static void main(String[] args) {
		Properties prop = new Properties();
//		prop.put("metadata.broker.list", "172.16.89.128:9092");
		prop.put("bootstrap.servers", "172.16.89.128:9092,172.16.89.129:9092,172.16.89.130:9092");
		prop.put("request.required.acks", 1);
		prop.put("serializer.class", "kafka.serializer.StringEncoder");
		prop.put("key.serializer", StringSerializer.class.getName());
		prop.put("value.serializer", StringSerializer.class.getName());
		
		KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(prop);
		
		for (int iCount = 0; iCount < 1000; iCount++) {
			String message = "My Test Message No " + iCount;
			ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("test-topic-tao", message);
			producer.send(record);
		}
		producer.close();
	}
}