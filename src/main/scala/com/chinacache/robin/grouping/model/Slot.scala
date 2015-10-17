package com.chinacache.robin.grouping.model

import scala.collection.mutable.ArrayBuffer

/**
 * Created by robinmac on 15-8-4.
 */
abstract class BasicSlot(val slotid:Long) {
  /**
   * The form like this
   *
   * topic1: channelid-userid1,channelid-userid2,channelid-userid3
   * topic2: channelid-userid4,channelid-userid5,channelid-userid6
   */
  val channelList=new ArrayBuffer[(String,Long)]

  def transformToMap(reflectid:Long=slotid):Iterable[(String,Long)] = channelList.map(_._1->reflectid)

  def getDataSize= channelList.map(_._2).sum
  def getChannelNum=channelList.size
}

class TopicSlot(slotid:Long,val batchType:Int) extends BasicSlot(slotid:Long){
  override def toString = "[Type]:"+batchType+"\t[Slotid]:"+slotid+"\t[ChannelNums]:"+channelList.size+"\t[DataSize]: "+getDataSize
  var boltid=0l;
}

class BoltSlot(slotid:Long) extends BasicSlot(slotid:Long){
  val tsList=new ArrayBuffer[TopicSlot]

  override def getDataSize=tsList.map(_.getDataSize).sum

  override def toString = "[BSlotid]:"+slotid+"\t[DataSize]: "+getDataSize+"\t[topicSlot Size:]:"+tsList.size

}
