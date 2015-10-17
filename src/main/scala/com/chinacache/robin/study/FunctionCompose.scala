package com.chinacache.robin.study

/**
 * Created by robinmac on 15-7-22.
 */
class FunctionCompose {
  def f(s: String) = "f(" + s + ")"
  def g(s: String) = "g(" + s + ")"

  def f2=f(_)
  def g2=g(_)
//  val fg= f _ compose g
  //fg2
  val fg1=f _ compose g _
  val fg2= f2 compose g2
}
object FunctionCompose extends App{
  val m=new FunctionCompose;
  println(m.fg2 ("3"))
}
