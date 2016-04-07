package com.scala.spark.streaming
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object SparkStreaming {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkStreaming")
    // creating streaming context
    val sc = new StreamingContext(conf,Seconds(1))
    
    val DStream = sc.socketTextStream("localhost",8000)
    val wordStream = DStream.flatMap(_.split(" "))
    val wordDStream = wordStream.map(word=>(word,1))
    val countStream = wordDStream.reduceByKey(_+_)
    
    println(countStream.print())
    
    sc.start()
    sc.awaitTermination()
    
    
    TwitterUtils.createStream(sc, None)
  }
}