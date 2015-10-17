package com.chinacache.robin.study

/**
 * Created by robinmac on 15-7-31.
 *
 * Trying to write all of the method in tail recursion
 */
class BasicAlgorithm {

  @scala.annotation.tailrec
  final def sum(ar: Array[Int], index: Int,sumv:Int):Int =
    if (index == ar.length )
      sumv
    else
       sum(ar, index + 1,sumv+ar(index))

  @scala.annotation.tailrec
  private def max(ar:Array[Int],xmax:Int,index:Int):Int=
    if(index==ar.length)
      xmax
    else
      max(ar,Math.max(xmax,ar(index)),index+1);

//  final def countChange(money: Int, coins: List[Int]): Int ={
//
//  }

  final def fibonacci(n: Int,a1:Int,a2:Int): Int={
    if(n<2)
      a1
    else
      fibonacci(n-1,a1,a2+a1)
  }

  def bubbleSort(ar: Array[Int]): Unit ={

  }

}
object BasicAlgorithm extends App{
  val m=new BasicAlgorithm
  val res=m.sum(Array(1,2,3,4,5,6,7,8,9),0,0)
  println(res)
  val maxv=m.max(Array(10,2,3,4,5,6,7,8,9),Int.MinValue,0)
  println(maxv)
  val feb=m.fibonacci(10,0,1);
  println(feb)
}
