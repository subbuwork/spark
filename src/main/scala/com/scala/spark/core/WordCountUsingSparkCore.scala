package com.scala.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountUsingSparkCore {
  def main(args:Array[String])={
    // creating spark conf object
    val conf = new SparkConf().setMaster("local").setAppName("WordCountUsingSparkCore")
    
    // creating sparkcontext using sparkconf object
    val sparkContext = new SparkContext(conf)
    
    // reading file from HDFS , we can read data file from other sources like amazon s3, apache flume ,kafka etc.. and from 
    // local source too
    
    val file = sparkContext.textFile("hdfs://localhost:9000/user/input/search.txt",1)
  
    val wordCount = file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    // saving result in HDFS, same way can save to amazon s3 etc..
    wordCount.saveAsTextFile("hdfs://localhost:9000/user/output/opt6")
    sparkContext.stop()
  }
  
}