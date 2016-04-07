package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CreatingSQLContext {
  def main(args:Array[String])={
    
    val conf = new SparkConf().setMaster("local").setAppName("CreatingSQLContext")
    val sparkContext = new SparkContext(conf)
    
    // creating sqlContext using an existing spark context
    val sqlContext = new SQLContext(sparkContext)
    
    // Using below implicitly convert RDD to DataFrame
    import sqlContext.implicits._
    
  }
}