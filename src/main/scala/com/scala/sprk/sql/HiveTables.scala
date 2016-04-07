package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object HiveTables {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("HiveTables")
    
    val ctx = new SparkContext(conf)
    
    val hctx = new HiveContext(ctx)
    val df = hctx.sql("select * from mytable")
    
   // hctx.sql("CREATE TABLE  IF NOT EXISTS src (key INT, value STRING)")
    
  }
} 