package skyler.tao.kafka.highlevelconsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerGroupExample {

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	
	public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
		this.topic = a_topic;
	}
	
	public static void main(String[] args) {
		
		if (args.length <= 3) {
			System.err.println("Please input arguments like: zookeeperip:port groupId topicName threadNum");
		}
		String zookeeper = args[0];
		String groupId = args[1];
		String topic = args[2];
		int threads = Integer.parseInt(args[3]);
		
		ConsumerGroupExample example = new ConsumerGroupExample(zookeeper, groupId, topic);
		example.run(threads);
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.out.println("Interrupted exception occur!");
		}
		
		example.shutDown();
	}
	
	public void shutDown() {
		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
		
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly!");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly!");
		}
	}
	
	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		executor = Executors.newFixedThreadPool(a_numThreads);
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber ++;
		}
	}
	
	private static ConsumerConfig createConsumerConfig (String a_zookeeper, String a_groupId) {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", a_zookeeper);
		prop.put("group.id", a_groupId);
		prop.put("zookeeper.session.timeout.ms", "400");
		prop.put("zookeeper.sync.time.ms", "200");
		prop.put("auto.commit.interval.ms", "1000");
		
		return new ConsumerConfig(prop);
	}
}
