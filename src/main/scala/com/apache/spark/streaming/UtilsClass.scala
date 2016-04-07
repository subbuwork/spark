package com.apache.spark.streaming

import org.apache.spark.streaming.{StreamingContext,Seconds}
import org.apache.spark.SparkConf

object UtilsClass {
  def createStreaingContext(appName:String):StreamingContext = {
    println("Creating streaing context for :::"+appName)
    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val sc   = new StreamingContext(conf,Seconds(5))
    return sc
  }
}