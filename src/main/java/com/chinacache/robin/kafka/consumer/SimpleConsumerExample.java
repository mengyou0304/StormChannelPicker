/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by robinmac on 15-9-21.
 */
public class SimpleConsumerExample {
	private static final Logger logger = LoggerFactory
		.getLogger(SimpleConsumerExample.class);

	private List<String> m_replicaBrokers = new ArrayList<String>();

	public SimpleConsumerExample() {
		m_replicaBrokers = new ArrayList<String>();
	}

	public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
		//Step.1 : Find the leader
		// find the meta data about the topic and partition we are interested in
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
		if (metadata == null) {
			logger.warn("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			logger.warn("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		System.out.println("leader : "+leadBroker);
		String clientName = "Client_" + a_topic + "_" + a_partition;
		//Step.2: Build the simple consumer based on leader that finded
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
		//Step.2.1: get the lastest offset
		long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
		System.out.println("Current Offset: "+readOffset);
//		readOffset+=500000l;
//		readOffset=93027405210l;
		//Step.3: Read until the maxReadNum.
		int numErrors = 0;
		while (a_maxReads > 0) {
//			System.out.println("get Offset as :"+readOffset);
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
			}
			//Start fetching
			FetchRequest req = new FetchRequestBuilder()
				.clientId(clientName)
				.addFetch(a_topic, a_partition, readOffset, 700000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
				.build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(a_topic, a_partition);
				logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5) break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for the last element to reset
//					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
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
					logger.warn("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();

//				ByteBuffer prefix=messageAndOffset.message().key();
//				byte[] bprefix=new byte[prefix.limit()];
//				prefix.get(bprefix);
//				System.out.println(new String(bprefix, "UTF-8"));
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				System.out.println(new String(bytes, "UTF-8"));
//				checker.checkTime(new String(bytes, "UTF-8"));
				numRead++;
				a_maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
//			System.out.println("continue....");
//			break;
		}
		if (consumer != null) consumer.close();
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
	                                 long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
			requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			logger.warn("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		logger.warn("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop:
		for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
					+ ", " + a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null) consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}

	public static void main(String args[]) {
		SimpleConsumerExample example = new SimpleConsumerExample();
		long maxReads =Long.MAX_VALUE;
		String topic = "zeus";
		int partition = 21;
		List<String> seeds = new ArrayList<String>();
		seeds.add("180.97.185.52");
		int port = 9092;
		try {
			example.run(maxReads, topic, partition, seeds, port);
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}
}
