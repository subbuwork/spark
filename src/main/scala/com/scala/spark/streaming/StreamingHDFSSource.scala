package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import twitter4j.StreamController

object StreamingHDFSSource {
  def main(args:Array[String]){
    
    println("this is  StreamingHDFSSource")
    
    
  }
  
  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingHDFSSource")
  // creating streaming context for text file reading from HDFS data source
  val streamContext = new StreamingContext(conf,Seconds(1))
  
  // creating DStream for text accessing from HDFS storage
  
  val DStream = streamContext.textFileStream("hdfs://localhost:9000/user/input/test.txt")
  DStream.print()
  val count = DStream.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  count.print()
  streamContext.start()
  streamContext.awaitTermination()
}