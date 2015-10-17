package com.chinacache.robin.study

/**
 * Created by robinmac on 15-7-22.
 */
class StudyApply {
  def testApply1(): Unit = {
    val v3 = BigInt("123121");
    StudyApply.companion
  }

  def apply() = {
    println("Class's defined Apply")
  }

  def test {
    println("Class's defined test")
  }
}
object StudyApply{
  var staticValue=1;
  def companion=println("I'm a companion");

  def stat{
    println("Companion's static method")
  }

  def apply() = {
    println("Companion's Apply")
    new StudyApply
  }

  var count = 0

  def incc = {
    count += 1
  }
}
object StudyApplyRunner extends App{
  //类名后面跟方法名，调用的是伴生对象的方法
  println("\n\nCalling Code: StudyApply.stat")
  StudyApply.stat

  //类名后面加括号，相当于调用伴生对象的apply方法
  println("\n\nCalling Code: val a = StudyApply()")
  val a = StudyApply()

  //类的实例后面跟方法名，调用的时类中定义的方法
  println("\n\nCalling Code: a.test")
  a.test

  //对象加括号相当于调用对象的apply方法
  println("\n\nCalling Code: println(a())")
  println(a())

  println("\n\nCalling Code: val b = StudyApply.apply()")
  val b = StudyApply.apply()
  println("b is :"+b)
  println("b.test is :"+b.test)
  println("a is :"+a.apply())

  //半生对象的方法调用类似于类的静态方法调用
  for(i <- 0 until 10){
    StudyApply.incc
  }
  //半生对象的属性调用相当于类的属性方法调用
  println(StudyApply.count)
}
