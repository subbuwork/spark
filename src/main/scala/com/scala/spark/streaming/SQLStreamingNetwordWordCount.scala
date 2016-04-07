package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object SQLStreamingNetwordWordCount {
  
  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("SQLStreamingNetwordWordCount").setMaster("local[2]")
    // creating streaming context using spark conf and interval time 10 sec
    val ssc = new StreamingContext(conf,Seconds(10))
    // creating dstream using sockettext stream
    val DStream = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
    
    val words = DStream.flatMap(_.split(" "))
    
    words.foreachRDD((rdd:RDD[String])=>{
      // creating sql context using spark context
      val sqlContext = SqlContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      // creating DataFrame from RDD
      val wordsFrame = rdd.map(word=>Record(word)).toDF()
      
      // creating temp table words from  wordsDataFrame
      wordsFrame.registerTempTable("words")
      
      val wordsCount = sqlContext.sql("select word,count(*) as total from words group by word")
     // println(s"----$time-----")
      wordsCount.show()
      
      
    })
    ssc.start()
    ssc.awaitTermination()
  }
}