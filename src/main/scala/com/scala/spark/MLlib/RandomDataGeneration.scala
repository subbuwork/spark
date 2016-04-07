package com.scala.spark.MLlib

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._
object RandomDataGeneration {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("RandomDataGeneration").setMaster("local")
    val context = new SparkContext(conf)
    
    val u = normalRDD(context,1000000L,10)
    val v = u.map(x=> 1.0+2.0*x)
    println(v.foreach { x => println(x) })
    
    
    
  }
}