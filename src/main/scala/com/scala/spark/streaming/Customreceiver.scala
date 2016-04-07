package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

object Customreceiver {
  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("Customreceiver").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    
    val DStream = ssc.receiverStream(new CustomReceiver("localhost",9999))
    println("hai i am here+++")
    val wordCount = DStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    println(wordCount.print())
    
    ssc.start()
    ssc.awaitTermination()
  }
}