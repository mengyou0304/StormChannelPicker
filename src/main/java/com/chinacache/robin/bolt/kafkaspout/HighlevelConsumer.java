package com.chinacache.robin.bolt.kafkaspout;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HighlevelConsumer {
	private static final Logger logger = LoggerFactory
			.getLogger(HighlevelConsumer.class);
	
	private final ConsumerConnector consumer;
	
	private List<ConsumerIterator<byte[], byte[]>> itlist;

	public List<ConsumerIterator<byte[], byte[]>> getItlist() {
		return itlist;
	}

	public HighlevelConsumer(String zkstring, String group, String topic,
			int threadnum) {
		logger.info("zkstring={}, group={}, topic={}", zkstring, group, topic);

		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(zkstring,
						group));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(threadnum));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		itlist = new ArrayList<ConsumerIterator<byte[], byte[]>>();
		for (KafkaStream<byte[], byte[]> ks_stream : streams) {
			ConsumerIterator<byte[], byte[]> it = ks_stream.iterator();
			itlist.add(it);
		}
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("rebalance.backoff.ms", "10000");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "4000");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {
		String ip="180.97.185.37";
		String groupid="CNC-11";
		if(args!=null&&args.length!=0)
			ip=args[0];
		if(args!=null&&args.length>1)
			groupid=args[1];
		HighlevelConsumer example = new HighlevelConsumer(
			ip+":2181/YRFS/YangZhou/CNC", groupid, "zeus", 10);
		System.out.println("==========\n\n finish init!");
		ConsumerIterator<byte[], byte[]> it1 = example.getItlist().get(0);
		while (it1.hasNext()) {
			System.out.println(new String(it1.next().message()));
		}
		example.shutdown();
	}
}
