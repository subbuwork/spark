package com.scala.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LogWordCount {
  def main(args:Array[String]){
 
    println("main method")
    
  }
  val conf = new SparkConf().setMaster("local").setAppName("LogWordCount")
  val context = new SparkContext(conf)
  
  val file = context.textFile("hdfs://localhost:9000/user/input/log.txt")
  val count = file.flatMap(_.split(" ")).map(word =>(word,1)).reduceByKey(_+_)
  
  count.saveAsTextFile("hdfs://localhost:9000/user/output/opt11")
}