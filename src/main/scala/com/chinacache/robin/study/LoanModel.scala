package com.chinacache.robin.study

import java.io._
import java.util.Scanner

/**
 * Created by robinmac on 15-7-23.
 */
class LoanModel {
  def withScancer(f :File, op:Scanner=>Unit): Unit ={
//    val b=new BufferedReader(new FileReader(f));
    val scanner =new Scanner(f);
    try{
      op(scanner)
    }finally{
      scanner.close();
    }
  }
}
object LoanModel extends App{
  val lm=new LoanModel();
  def tmpFun(scan: Scanner)=println("pid is " + scan.next())

  lm.withScancer(new File("/Users/robinmac/workspace/testspace/a.out"),tmpFun)
  lm.withScancer(new File("/Users/robinmac/workspace/testspace/a.out"),func=>println("haha"+func.next()))

}
