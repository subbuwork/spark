package com.apache.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer._

object KafkaWordcount {
  def main(args: Array[String]){
    val Array(brokers1,topics)= args
    val topicsSet = topics.split(",").toSet
    
    val broker   = Map[String,String]("metadata.broker.list" -> brokers1)
    
    val conf     = new SparkConf().setMaster("local").setAppName("KafkaWordcount")
    val ssc      = new StreamingContext(conf,Seconds(5))
    
    // Create direct kafka stream with brokers and topics
    
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,broker,topicsSet)
    
    val lines    = messages.map(_._2)
    val words    = lines.flatMap(_.split(" "))
    val pairs    = words.map(word=>(word,1))
    val count    = pairs.reduceByKey(_+_)
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}