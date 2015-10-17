package com.chinacache.robin.kafka

import java.util.{Properties, Timer, TimerTask}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.Logging
import org.apache.hadoop.fs.Path

/**
 * The way we use it,like the Code:
 *
 * kc = new KafkaProducerConnection(topicid, AllConfiguration.KAFKA_INNER_BROKER)
 * kc.buildConnection
 * kc.sendFile(event.getLocalFile)
 * kc.close
 *
 *
 * @param topic
 * @param brokers
 */
class KafkaProducerConnection(topic: String = "testgroup",
                              brokers: String = "10.20.73.187:9092,10.20.73.188:9092,10.20.73.189:9092"
		                             ) extends Logging {
	val producer = buildConnection()
	var num = 0l;

	def sendFile(path: Path): Unit = {

	}

	def onEvent(msg: String, key: String): Unit = {
		num = num + 1l;
		val data = new KeyedMessage[String, String](topic, key, "msg");
		producer.send(data);
	}

	def close() = producer.close()

	def buildConnection(): Producer[String, String] = {
		val props = new Properties()
		props.put("metadata.broker.list", brokers)
		props.put("serializer.class", "kafka.serializer.StringEncoder")
		props.put("producer.type", "async")
		//		props.put("partitioner.class", "com.robin.kafka.ConfigBasedPartitioner")
		props.put("compression.codec", "gzip")

		props.put("batch.size", "16384")
		val k = 1024 * 1024 * 8
		props.put("send.buffer.bytes", k.toString)
		//async only
		props.put("queue.buffering.max.messages", "40000")
		props.put("batch.num.messages", "1500")


		val config = new ProducerConfig(props)
		val producer = new Producer[String, String](config)
		producer
	}

	def startMonitor(): Unit = {
		val timer = new Timer();
		val logPrintInterval: Int = 3000
		timer.schedule(new TimerTask {
			var lastnum = num;

			override def run(): Unit = {
				println("time in each " + logPrintInterval / 1000 + " seconds" + (num - lastnum))
				lastnum = num;
			}
		}, 1000l, logPrintInterval);
	}

}

object ScalaProducerExample extends App {
	val c1 = new KafkaProducerConnection(topic = "testtopic5")
	c1.startMonitor()
	(1 to 400000).foreach(v => {
		c1.onEvent("new message" + v, "new key")
	})
}