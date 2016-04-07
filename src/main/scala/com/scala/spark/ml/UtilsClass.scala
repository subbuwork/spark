package com.scala.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object UtilsClass {
  
  def getSparkContext(appName:String):SparkContext={
    println("creating saprk context for ::"+appName)
    val conf = new SparkConf().setMaster("local").setAppName(appName)
    val context = new SparkContext(conf)
    return context
  }

def createSQLContext(appName:String):SQLContext={
  println("creating sql context for :::"+appName)
   val conf = new SparkConf().setMaster("local").setAppName(appName)
    val context = new SparkContext(conf)
  val sqlContext = new SQLContext(context)

return sqlContext
}

def sourcePath():String={
  val sourcePath = "hdfs://localhost:9000/user/sparkdata/mllib/"
return sourcePath
}

}