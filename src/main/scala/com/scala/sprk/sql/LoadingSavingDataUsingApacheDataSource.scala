package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object LoadingSavingDataUsingApacheDataSource {
  def main(args:Array[String])={
    
    val conf = new SparkConf().setMaster("local").setAppName("LoadingSavingDataUsingApacheDataSource")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    
    val df =sqlContext.read.format("json").load("hdfs://localhost:9000/user/input/people.json")
    df.select("name","age").write.format("json").save("hdfs://localhost:9000/user/output/opt8")
    
  }
}