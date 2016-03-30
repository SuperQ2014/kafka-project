package skyler.tao.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
 
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleConsumerExample {
	
	/*
	 * maxReads: 20
	 * partitionId: 3
	 * consuming 475 messages
	 * 
	 */
	public static void main(String[] args) {
		
		if (args.length <= 4) {
			System.err.println("Please add arguments like: maxReads topic partitionId brokerIP port");
			return;
		}
		
		SimpleConsumerExample example = new SimpleConsumerExample();
		long maxReads = Long.parseLong(args[0]);
		String topic = args[1];
		int partition = Integer.parseInt(args[2]);
		List<String> seeds = new ArrayList<String>();
		seeds.add(args[3]);
		int port = Integer.parseInt(args[4]);
		try {
			example.run(maxReads, topic, partition, seeds, port);
		} catch (Exception e) {
			System.out.println("Oops: " + e);
			e.printStackTrace();
		}
	}
	
	private void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
		
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
		if (metadata == null) {
			System.out.println("Cannot find Medadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out.println("Cannot find Medadata for Topic and Partition. Exiting");
			return;
		}
		
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + a_topic + "_" + a_partition;
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 1000000, 64*1024, clientName);
		long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
		
		int numErrors = 0;
		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64*124, clientName);
			}
			FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientName)
										.addFetch(a_topic, a_partition, readOffset, 100000).build();
			FetchResponse fetchResponse = consumer.fetch(fetchRequest);
			
			if (fetchResponse.hasError()) {
				numErrors++;
				short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker: " + leadBroker + " Reason: " + code);
				if (numErrors > 5) break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
				continue;
			}
			numErrors = 0;
			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();	//message存储在java.nio.HeapByteBuffer里,通过payload信息获取message
				
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);	//message 存储为bytes数组
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
				numRead++;
				a_maxReads--;
			}
			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}
		}
		if (consumer != null) consumer.close();
	}

	private List<String> m_replicaBrokers = new ArrayList<String>();
	
	private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
		
		PartitionMetadata returnMetadata = null;
		loop:
			for (String seed : a_seedBrokers) {
				SimpleConsumer consumer = null;
				try {
					consumer = new SimpleConsumer(seed, a_port, 100000, 64*1024, "leaderLookup");
					List<String> topics = Collections.singletonList(a_topic);
					TopicMetadataRequest request = new TopicMetadataRequest(topics);
					TopicMetadataResponse response = consumer.send(request);
					
					List<TopicMetadata> metadata = response.topicsMetadata();
					
					for (TopicMetadata item : metadata) {
						for (PartitionMetadata part : item.partitionsMetadata()) {
							if (part.partitionId() == a_partition) {
								returnMetadata = part;
								break loop;
							}
						}
					}
				} catch (Exception e) {
					System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + 
							a_topic + ", " + a_partition + "] Reason: " + e);
				} finally {
					if (consumer != null)
						consumer.close();
				}
			}
		if (returnMetadata != null) {
			m_replicaBrokers.clear();
			for (Broker replica : returnMetadata.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetadata;
	}
	
	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if (response.hasError()) {
			System.out.println("Error fetching data Offset. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
		
		for (int i = 0; i < 3; i++) {
			boolean gotoSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				gotoSleep = true;
			} else if(metadata.leader() == null) {
				gotoSleep = true;
			} else if(a_oldLeader.equals(metadata.leader().host()) && i == 0) {
				gotoSleep = true;
			} else {
				return metadata.leader().host();
			}
			
			if (gotoSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {}
			}
			
			System.out.println("Unable to find new leader after Broker failure. Exiting");
			throw new Exception("Unable to find new leader after Broker failure. Exiting");
		}
		return null;
	}
}