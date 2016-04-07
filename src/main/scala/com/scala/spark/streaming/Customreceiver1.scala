package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Customreceiver1 {
  def main(args:Array[String])={
    val conf = new SparkConf().setMaster("local[2]").setAppName("Customreceiver1")
    val ssc = new StreamingContext(conf,Seconds(5))
    val DStream = ssc.receiverStream(new CustomReceiver1("localhost",9999))
    val wordCount = DStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    println(wordCount.print())
    ssc.start()
    ssc.awaitTermination()
  }
}