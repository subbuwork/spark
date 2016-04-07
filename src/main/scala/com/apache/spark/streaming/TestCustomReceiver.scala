package com.apache.spark.streaming

object TestCustomReceiver {
  def main(args: Array[String]): Unit = {
    val sc = UtilsClass.createStreaingContext("TestCustomReceiver")
    
    val lineDSStream  = sc.receiverStream(new TestReceiver("localhost",9999))
    val countDSStream = lineDSStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_).print()
    sc.start()
    sc.awaitTermination()
  }
}