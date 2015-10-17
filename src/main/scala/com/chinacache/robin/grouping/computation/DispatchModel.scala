package com.chinacache.robin.grouping.computation

/**
 * Created by robinmac on 15-8-6.
 */
abstract class DispatchModel (val start:Int,val end:Int){
  var value=start

  def change():Int

}
class DispatchByW(start:Int,end:Int) extends DispatchModel(start,end){
  var step=1
  println("start="+start+" end="+end)
  override def change(): Int = {
    value+=step
    if(value>=end||value<start){
      step=(-step)
      value+=step
    }
    value
  }
}
object DispatchByW extends App{
  val k=new DispatchByW(0,200)
  for(i<-0 to 600){
    println(k.value)
    k.change()
  }
  val ar=new Array[DispatchByW](3)
  println(ar(0)==null)
  println(ar(0))
}
