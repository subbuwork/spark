package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object SparkHdfsWordCount {
  
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val context = new SparkContext(conf)
    
    // Reading file from HDFS
    val file = context.textFile("hdfs://localhost:9000/user/input/test.txt", 1)
    val count = file.flatMap(line =>line.split(" ")).map(word =>(word,1)).reduceByKey(_+_)
    // Save output in Hdfs
    count.saveAsTextFile("hdfs://localhost:9000/user/ouput/opt4")
    
    
  }
}
