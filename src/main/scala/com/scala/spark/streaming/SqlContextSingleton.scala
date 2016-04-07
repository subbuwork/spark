package com.scala.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object SqlContextSingleton {
  @transient private var instance: SQLContext = _
  def getInstance(sparkContext:SparkContext): SQLContext ={
    if(instance == null){
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}