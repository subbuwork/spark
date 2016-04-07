package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext,Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object SqlNetworkWordCount {
  def main(args:Array[String]){
    println("======>SqlNetworkWordCount====>")
  
  val conf = new SparkConf().setAppName("").setMaster("local[2]")
  val context = new StreamingContext(conf,Seconds(10))
  val lines = context.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER)
  val wordsStream = lines.flatMap(_.split(" "))
  
  wordsStream.foreachRDD((rdd:RDD[String],time:Time)=>{
    
  val sqlContext = SqlContextSingleton.getInstance(rdd.sparkContext)
  import sqlContext.implicits._
  
  // converting from RDD to DataFrame
  val wordFrame = rdd.map(record=>Record(record)).toDF()
  //converting dataframe to table
      wordFrame.registerTempTable("words")
  // quering table     
  val wordCountDataFrame=sqlContext.sql("select word,count(*) as total from words group by word")
  println(s"===>===>$time==>===>===>====>===>")
  wordCountDataFrame.show()
  
  })
  context.start()
  context.awaitTermination()
  }
}