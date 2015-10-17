package com.chinacache.robin.study.scalajava

/**
 * Created by robinmac on 15-8-16.
 */
class ScalaCode {
	def basicDemos(): Unit ={
		//reduce
		val ars=Array(1,2,3,4,5,6,7,8)
		val sum=ars.reduce((a,b)=>{
			a+b
		})
		val sum2=ars.reduce(_+_)
		println("sum="+sum+"  sum2="+sum2)

		//for each
		var sum3=0
		var sum4=0
		val ars2=Array(1,2,3,4,5,6,7,8)
		ars2.foreach(v=>{
			sum3+=v
		})
		println("sum3="+sum3)

		ars2.foreach(sum4+=_)
		println("sum4="+sum4)

		//map
		val ars3=Array(1,2,3,4,5,6,7,8)
		val ars3_new=ars3.map(v=>v*2)
		print("After Chang the form, the Array(1,2,3,4,5,6,7,8) change to :")
		ars3_new.foreach(v=>print(v+" "))



	}

}
object ScalaCode extends App{
	val s=new ScalaCode()
	s.basicDemos()

}
