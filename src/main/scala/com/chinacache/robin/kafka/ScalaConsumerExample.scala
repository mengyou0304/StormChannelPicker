package com.chinacache.robin.kafka

import java.util.concurrent._
import java.util.{Properties, Timer, TimerTask}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.utils.Logging


class ScalaConsumerExample(val zookeeper: String = "180.97.185.39:2181/YRFS/YangZhou/CNC",
                           val groupId: String = "testss",
                           val topic: String = "test") extends Logging {

  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)

  var executor: ExecutorService = null
  var receviedNum = 0l;

  def shutdown() = {
    if (consumer != null)
      consumer.shutdown();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "largest");
    props.put("zookeeper.session.timeout.ms", "2000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("rebalance.backoff.ms", "10000")
    props.put("zookeeper.connection.timeout.ms", "10000")
    props.put("auto.offset.reset", "smallest");
    print(props)
    val config = new ConsumerConfig(props)
    config
  }

  def run(numThreads: Int) = {

    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap);
    val streams = consumerMap.get(topic).get;

    executor = Executors.newFixedThreadPool(numThreads);
    var threadNumber = 0;
    for (stream <- streams) {
      executor.submit(new ScalaConsumerTest(stream, threadNumber, 1))
      threadNumber += 1
    }
  }

  class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val delay: Long) extends Logging with Runnable {
    var currentNum = 0l
    new Timer().schedule(new TimerTask {
      var lastnum = 0l
      override def run(): Unit = {
        println("consume event Num: " + (currentNum - lastnum) + " ThreadNumber: " + threadNumber)
        lastnum = currentNum
      }
    }, 1000l, 3000l)

    def run {
      println("start Running!")
      val it = stream.iterator()
      while (it.hasNext()) {
        val msg = new String(it.next().message())
        currentNum += 1
      }
      println("Shutting down Thread: " + threadNumber);
    }

  }

}

object ScalaConsumerExample extends App {
    val example = new ScalaConsumerExample()
    example.run(1)
}


