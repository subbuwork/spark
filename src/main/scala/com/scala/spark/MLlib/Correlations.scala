package com.scala.spark.MLlib

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

object Correlations {
  def main(args:Array[String]){
  
  val conf = new SparkConf().setMaster("local").setAppName("Correlations")
  val context = new SparkContext(conf)
  
  val seriesX:RDD[Double] = context.parallelize(Seq(1.0,2.0,3.0,4.0)) // a series
  val seriesY:RDD[Double] = context.parallelize(Seq(2.0,4.0,5.0,6.0)) // must have the same number of partitions and cardinality as seriesX
  
  // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
  // method is not specified, Pearson's method will be used by default. 
  
  val correlation:Double = Statistics.corr(seriesX, seriesY,"pearson")
  println("Correlation value::"+correlation)
  
  // calculating correlation using spearman method
  val correlation1:Double = Statistics.corr(seriesX, seriesY,"spearman")
  println("Correlation1 value::"+correlation1)
  
  
  // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
  // If a method is not specified, Pearson's method will be used by default. 
  
  val v = Vectors.dense(Array(3.0,4.0,6.0,3.0,8.0))
  val v2 = Vectors.dense(Array(5.0,1.0,7.0,2.0,6.0))
  val v3 = Vectors.dense(Array(7.0,9.0,1.0,6.0,2.0))
  val vector:RDD[Vector] = context.parallelize(Seq(v,v2,v3))
  
  val correlationMatrix = Statistics.corr(vector,"pearson")
  println("correlationMatrix:::"+correlationMatrix)
  
  // correlation using spearman method
  
  val spearmanCorrelatoin = Statistics.corr(vector, "spearman")
  println("spearmanCorrelatoin::::"+spearmanCorrelatoin)
  
  }
}