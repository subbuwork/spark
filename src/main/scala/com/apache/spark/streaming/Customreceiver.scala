package com.apache.spark.streaming

object Customreceiver {
  def main(args: Array[String]): Unit = {
    val sc = UtilsClass.createStreaingContext("Customreceiver")
    
    val linesDStream = sc.receiverStream(new CustomReceiver("localhost",9999))
    val wordsDStream = linesDStream.flatMap(_.split(" "))
    val countDStream = wordsDStream.map(x =>(x,1)).reduceByKey(_+_)
    countDStream.print()
    sc.start()
    sc.awaitTermination()
  
  }
}