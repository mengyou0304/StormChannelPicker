package com.chinacache.robin.kafka

/**
 * Created by robinmac on 15-7-20.
 */

object CreatingTopicScript extends App {
	val baseString = "/usr/lib/kafka_2.11-0.8.2.1/bin/kafka-topics.sh --zookeeper 10.20.73.186:2181,10.20.73.187:2181,10.20.73.188:2181,10.20.73.189:2181,10.20.73.190:2181 --create  --partitions 1 --replication-factor 2 --topic topictest";
	for (i <- 1 to 300)
		println(baseString + i)


}
