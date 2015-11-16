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
public class LagChecker {
	private static final Logger logger = LoggerFactory
		.getLogger(LagChecker.class);

	private List<String> m_replicaBrokers = new ArrayList<String>();
	MsgTimeChecker checker=null;

	public boolean checkNewMessage=true;
	public boolean printMessage=false;
	public boolean parseKey=true;
	public boolean parseMessage=true;

	public LagChecker() {
		m_replicaBrokers = new ArrayList<String>();
	}


	public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port, String part) throws Exception {
		checker=new MsgTimeChecker(a_topic,a_partition,part);
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
		System.out.println("Origin leader : "+leadBroker);
		System.out.println("Reflected leader : "+leadBroker);
		String clientName = "Client_" + a_topic + "_" + a_partition;
		//Step.2: Build the simple consumer based on leader that finded
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
		//Step.2.1: get the lastest offset
		long readOffset=0l;
		if(checkNewMessage)
			readOffset= getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);

		System.out.println("Current Offset: "+readOffset);
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
					if(checkNewMessage)
						readOffset= getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					else
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

				byte[] bprefix=new byte[0];
				byte[] bytes=new byte[0];

				if(parseKey) {
					ByteBuffer prefix = messageAndOffset.message().key();
					if(prefix!=null) {
						bprefix = new byte[prefix.limit()];
						prefix.get(bprefix);
					}
				}
				if(parseMessage) {
					ByteBuffer payload = messageAndOffset.message().payload();
					bytes = new byte[payload.limit()];
					payload.get(bytes);
				}
				if(printMessage)
					logger.info("[" + new String(bprefix, "UTF-8") + "]"+new String(bytes, "UTF-8"));
				checker.checkTime(new String(bytes, "UTF-8"));
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
		OffsetRequest request = new OffsetRequest(
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
				TopicMetadataResponse resp = consumer.send(req);

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
	public static List<String> getSeedByPart(String part){
		List<String> seeds = new ArrayList<String>();
		if(part.equals("CNC"))
			seeds.add("153.99.250.52");
		if(part.equals("CHN"))
			seeds.add("180.97.185.52");
		return seeds;
	}

	public static void main(String args[]) {
		int st=30;
		int ed=32;
		String part="CNC";
		final List<String> seeds =new ArrayList<String>();
		seeds.add("180.97.185.52");
		if(args.length>3){
			seeds.clear();
			st=Integer.parseInt(args[0]);
			ed=Integer.parseInt(args[1]);
			part=args[2];
			seeds.add(args[3]);
		}
		try {
			for(int i=st;i<ed;i++) {
				final int finalI = i;
				final String finalPart = part;
				Runnable checkThread=new Runnable(){
					@Override
					public void run() {
						long maxReads =Long.MAX_VALUE;
						String topic = "zeus";
						int partition = finalI;
						if(seeds==null||seeds.size()==0)
							seeds.addAll(getSeedByPart(finalPart));
						int port = 9092;
						LagChecker checker = new LagChecker();
						try {
							System.out.println("check: "+topic+"\t"+partition+"\t"+seeds+"\t"+port+"\t"+finalPart);
							checker.run(maxReads, topic, partition, seeds, port, finalPart);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				};
				Thread d=new Thread(checkThread);
				d.start();
			}
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}
}
