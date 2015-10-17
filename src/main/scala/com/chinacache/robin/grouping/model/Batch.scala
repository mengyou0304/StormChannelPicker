package com.chinacache.robin.grouping.model

import com.chinacache.robin.grouping.computation.DispatchByW

import scala.collection.mutable.ArrayBuffer

/**
 *
 * Created by robinmac on 15-8-4.
 */
class Batch(infos: List[(String, Long)], slotNum: Long, startid: Long, batchType:String) {
  val bdescription=new BatchDescrition(batchType);

  override def toString: String =
  "\nBatch: [" + startid + "~~" + (startid + slotNum - 1) + "]\t [type]"+bdescription.typeNum+":\n"+
  "Channel Num:"+infos.length+"\n"+
  "Channel Size: "+infos.map(_._2).sum/1024+"G\n\n"+
    assignSlotsInBatchAndGet().map(b=>{
      b.toString+"\n"
    })

  /**
   *
   * @return
   */
  def assignSlotsInBatchAndGet(): ArrayBuffer[TopicSlot] = {
    val slotList = new ArrayBuffer[TopicSlot]
    for (id <- startid to slotNum + startid - 1) {
      slotList += new TopicSlot(id,bdescription.typeNum)
    }
    var k = new DispatchByW(0,slotList.size)
    var step = 1
    infos.sortWith((a,b)=>(a._2-b._2>0)).foreach(channel => {
      slotList(k.value).channelList += channel
      k.change()
    })
    slotList
  }
  class BatchDescrition(val batchType:String){
    val ss:Array[String]=batchType.split(":");
    val typeNum=ss(0).toInt
    val channelNum=ss(1).toInt
    val dataSizePSlot=ss(2).toInt
  }
}

