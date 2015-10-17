/*
 * Copyright (c) 2015.
 * All rights reserved by you.meng@chinacache
 *
 */

package com.chinacache.robin.grouping.computation

import java.io.PrintWriter

import com.chinacache.robin.grouping.model.{Batch, BoltSlot, TopicSlot}
import kafka.utils.Logging

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

/**
 *
 * c: Channel
 * s/b: Slot/Blot
 * num: boundary based batch
 *
 *
 * c1 c2 c3       c4 c5 c6 c7
 * |     |        |        |
 * +-----+        +--------+
 * s1              s2
 * |               |
 * +---------------+
 * b1
 *
 * ================================
 *
 * s1   s2 s3      s4 s5 s6 s7 s8
 * |        |      |            |
 * +--------+      +------------+
 * v1 num1  v2     v1   num2    v2
 *
 * So:
 * v1,v2,v3,v4: Boundaries
 * Num1,Num2  : slotNums in Batch
 * c1,c2,c3->s1  :Slot
 * s1,s2->b1     :Bolt
 * How to spread s1~s3 into Num1: SlotSpreadingAlgorithm
 * How to spread s1~s8 into Num1,Num2,Num3 : AssignmentAlgorithm
 *
 * Created by robinmac on 15-7-30.
 */
class SizeComputation(url: String,
                      topicNum: Int,
                      boltNum: Int
		                     ) extends Logging {


	val readHDFSSize =
		addAddtionalHDFSFile(_: HashMap[String, Long], url)
	// batchNames: "id:size(G):"
	val batchNames = Array("0:0:0", "1:200:1", "2:90:10", "3:18:360")

	/**
	 * Read DataSize infos from hdfsoutput file.
	 * It also support more than one file, which would add the size of them together by key
	 *
	 * @param dataSizeMap size mapping like channel1->size1,channel2->size2...
	 * @param additionalURL adding some additional file url like the form of dataSizeMap
	 * @return new Mapping
	 */
	def addAddtionalHDFSFile(dataSizeMap: HashMap[String, Long], additionalURL: String): HashMap[String, Long] = {
		val lines = Source.fromFile(additionalURL).getLines().toList
		for (line <- lines) {
			val ss = line.split(" ")
			val key = getKey(ss(2).split("[/]"))
			if (!dataSizeMap.contains(key)) {
				dataSizeMap(key) = 0
			}
			val value = ss(0).toLong >> 20
			dataSizeMap(key) += value
		}
		logger.info("Summed size: " + dataSizeMap.values.sum + "\n")
		dataSizeMap
	}

	def getKey(sArray: Array[String]): String =
		sArray(5) + "-" + sArray(6)

	def startAll(): Array[(String, Long)] = {
		val sizeMap = readHDFSSize(new HashMap[String, Long])
		val ab=startAssignment(sizeMap, writeToFile(_, url + ".topicmapping"), writeToFile(_, url + ".boltmapping"))
		ab.toArray
	}

	def startAssignment(infomap: HashMap[String, Long],
	                    topicAction: (Iterable[(String, Long)]) => Unit,
	                    boltAction: (Iterable[(String, Long)]) => Unit
			                   ): ArrayBuffer[(String, Long)] = {

		val topicSlotList: ArrayBuffer[TopicSlot] = startAssignTopicSlot(infomap)
		val topicMapping = topicSlotList.flatMap(slot => slot.transformToMap())
		topicAction(topicMapping)

		val boltSlotList: ArrayBuffer[BoltSlot] = startAssignBoltSlot(topicSlotList)
		val boltmappinglist = new ArrayBuffer[(String, Long)]
		boltSlotList.foreach(bslot => {
			logger.info(bslot)
			bslot.tsList.foreach(tslot => {
				val values = tslot.transformToMap(bslot.slotid)
				//        values.foreach(println)
				boltmappinglist.appendAll(values)
			})
		})
		boltAction(boltmappinglist)
		boltmappinglist
	}

	/**
	 * Put the topic Slots into BoltSlots using W-Dispatch
	 * @param topicSlotList
	 * @return
	 */
	def startAssignBoltSlot(topicSlotList: ArrayBuffer[TopicSlot]): ArrayBuffer[BoltSlot] = {
		var datasum = 0l;
		var channelNum = 0l;
		topicSlotList.foreach(v => {
			datasum += v.getDataSize
			channelNum += v.getChannelNum
		})
		val avgDataSize = datasum / boltNum
		println("avg Datasize: " + avgDataSize)
		val res = new ArrayBuffer[BoltSlot](boltNum)
		for (i <- 0 to boltNum)
			res += new BoltSlot(i)

		var currentBoltSize = 0l
		var cp = new DispatchByW(0, boltNum)
		topicSlotList.sortWith(_.getDataSize > _.getDataSize).foreach(slot => {
			//      println(slot)
			logger.debug(slot + " --- cp=" + cp.value + " size:" + res(cp.value).getDataSize)
			var dataSize = slot.getDataSize
			do {
				val originalSize = res(cp.value).getDataSize
				res(cp.value).tsList += slot
				val filledSize = avgDataSize - originalSize
				dataSize += -filledSize
				cp.change()
				//        if(slot.batchType==3&&firsttime){
				//          firsttime=false
				//          val bounds = assignBoundaries()
				//          val magicValue=boltNum-assignSlotNums(bounds).sum
				//          cp=new DispatchByW(magicValue, boltNum)
				//        }
			} while (dataSize > 0 && res(cp.value).getDataSize < avgDataSize)
		})
		res
	}

	def startAssignTopicSlot(infomap: HashMap[String, Long]): ArrayBuffer[TopicSlot] = {
		//boundaries of each batch
		val bounds = assignBoundaries(infomap)
		// nums of each batch
		val slotnums = assignSlotNums(bounds)
		// All batches
		val batchlist = new ArrayBuffer[Batch]
		// sorted channels
		var starter = 0l
		for (i <- 1 to bounds.length - 1) {
			val batchinfo: List[(String, Long)]
			= infomap.toList.filter(v => {
				v._2 > bounds(i) && v._2 <= bounds(i - 1)
			})
			batchlist += new Batch(batchinfo, slotnums(i - 1), starter, batchNames(i))
			starter += slotnums(i - 1)
		}
		val topicSlotList = new ArrayBuffer[TopicSlot]
		batchlist.foreach(v => {
			topicSlotList.appendAll(v.assignSlotsInBatchAndGet())
			logger.info(v.toString)
		})
		topicSlotList
	}

	/**
	 * Determine the slotSums for each boundary:
	 *
	 * @param boundaries
	 * @return
	 */
	def assignSlotNums(boundaries: Array[Long]): Array[Long] = {
		Array(topicNum / 6, topicNum * 2 / 3, topicNum / 6)
	}

	def writeToFile(map: Iterable[(String, Long)], fileurl: String): Unit = {
		val out = new PrintWriter(fileurl)
		map.foreach(v => out.println(v._1 + "=" + v._2))
		out.close()
		logger.info("Finish writing configureations into " + fileurl)
	}

	def satasticSizeInfo(): Array[Long] = {
		logger.debug("Starting Staging-based satastic")
		val volums = Array(0l, 0l, 0l, 0l)
		val sizes = Array(0, 0, 0, 0)
		val dataSizeMap = readHDFSSize(new HashMap[String, Long])
		val bounds = assignBoundaries(dataSizeMap)
		for (i <- 1 to bounds.length - 1)
			sizes(i) = dataSizeMap.toList.filter(
				v => v._2 match {
					case v2 if v2 > bounds(i) && v2 <= bounds(i - 1) => true
					case _ => false
				}
			).map(v => {
				volums(i) += v._2
			}
					).size
		logger.info("All Channel Size: " + dataSizeMap.size)
		for (i <- 1 to bounds.length - 1)
			logger.info("[" + bounds(i - 1) + "~~" + bounds(i) + "]Channel Num:" + sizes(i) + "  ChannelSize:" + volums(i))
		bounds
	}

	/**
	 * Determine the boundaries of the batches, from here, we directly point out the
	 * precise value of each boundary.
	 * @param infomap the data size of each channel
	 * @return the boundaries of each
	 */
	def assignBoundaries(infomap: HashMap[String, Long] = readHDFSSize(new HashMap[String, Long])): Array[Long] =
		Array(Integer.MAX_VALUE, 200000, 1000, -1)


}

object SizeComputation extends App {
	val url = "/Application/nla/log_pick/conf/test/size.out";
	val sc = new SizeComputation(url, 300, 100)
	sc.startAll()
	sc.satasticSizeInfo()

	//  if(!topicInfos.topicMapping.isEmpty)
	//    sc.rebalance(topicInfos.topicMapping)
}
