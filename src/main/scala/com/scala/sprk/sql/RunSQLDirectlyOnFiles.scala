package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object RunSQLDirectlyOnFiles {
  def main(args:Array[String])={
     val conf = new SparkConf().setMaster("local").setAppName("LoadingSavingDataUsingApacheDataSource")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
     
    val result=sqlContext.sql("select * from json.`hdfs://localhost:9000/user/input/people.json`")
    result.foreach(println)
  }
}