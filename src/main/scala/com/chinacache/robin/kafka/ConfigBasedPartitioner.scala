package com.chinacache.robin.kafka

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

import scala.io.Source

/**
 * Created by robinmac on 15-7-30.
 */
class ConfigBasedPartitioner(props: VerifiableProperties = null) extends Partitioner {
	//  val url=props.getString("partition.config.url")
	val url = "/Application/nla/log_pick/conf/test.out"
	val lines: List[String] = Source.fromFile(url).getLines().toList
	val res=lines.map(v=>{(v.split("=")(0))->(v.split("=")(1))})
	println("After reading the configruration,we get partition Map:\n" + res)

	override def partition(key: Any, numPartitions: Int): Int = {
//		map.get(key.toString).getOrElse(0)
		1
	}
}

object ConfigBasedPartitioner extends App {
	val k = new ConfigBasedPartitioner();
}


