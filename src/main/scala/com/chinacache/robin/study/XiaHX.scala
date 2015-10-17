package com.chinacache.robin.study

/**
 * Created by robinmac on 15-7-22.
 */
class XiaHX {
  def twop(x:Int,y:Int)=x+y
  val twop2=((x:Int,y:Int)=>x+y).curried
  val cc=(twop _).curried;

  def add(x:Int,y:Int)=x+y
  val add3=add(3,_:Int)

  val add5=add(5,_:Int)
}
object XiaHX extends App {
  val m=new XiaHX();
  val twopt=m.twop(1,_:Int);
  val twop33=m.twop2(1)

  println(twopt(1))
  println(m.cc(1)(2))
  println(m.twop2(1)(2))
  println(twop33(4))
  println(m.add5(3));


}
