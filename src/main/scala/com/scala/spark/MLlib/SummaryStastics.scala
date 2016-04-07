package com.scala.spark.MLlib

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary,Statistics}

object SummaryStastics {
  
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("SummaryStastics")
    val context = new SparkContext(conf)
    
    // creating vectors 
    val v1 = Vectors.dense(Array(1.0,2.0,3.0,4.0))
    val v2 = Vectors.dense(Array(3.0,5.0,7.0,5.0))
    val v3 = Vectors.dense(Array(8.0,3.0,1.0,0.0))
    
    // Creating an RDD of Vectors
    val observations = context.parallelize(Seq(v1,v2,v3))
    
    // Compute column summary statistics
    
    val summary:MultivariateStatisticalSummary = Statistics.colStats(observations)
  
    // Column summary
    println("Column mean value::"+summary.mean)
    println("Column max value::"+summary.max)
    println("Columan min value:::"+summary.min)
    println("Column variance::"+summary.variance)
    println("Column nonzeros::"+summary.numNonzeros)
    println("Column total count::"+summary.count) 
  }
}