package com.scala.spark.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object NetworkStreaming {
  def main(args:Array[String]){
    
    val HOST = "localhost"
    val PORT = 9999
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkStreaming")
    // creating streaming context with 5 secs wait time
    val sc = new StreamingContext(conf,Seconds(5))
    
    // Creating DStream
    val DStream = sc.socketTextStream(HOST,PORT);
    
    val count = DStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    
    count.print()
    
    sc.start()
    sc.awaitTermination()
    
    
  }
}