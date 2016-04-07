package com.scala.spark.graphx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

object GraphOperators {
  def main(args:Array[String])={
     val conf = new SparkConf().setMaster("local").setAppName("DeconstructiongGraph")
    val context = new SparkContext(conf)
    
    // val graph:Graph[(String,String),Sring]
  }
}