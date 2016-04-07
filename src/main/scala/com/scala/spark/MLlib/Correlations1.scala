package com.scala.spark.MLlib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
//import org.apache.spark.ml.param.Params
object Correlations1 {
  
 def main(args:Array[String]){
   val conf = new SparkConf().setMaster("local").setAppName("Correlations1")
   val context = new SparkContext(conf)
   
   val examples = MLUtils.loadLibSVMFile(context, "hdfs://localhost:9000/user/sparkdata/mllib/linear_regression_data.txt").cache()
   println(s"${examples.count()} data points")
   
   // Calculate label -- feature correlations
   
   val labelRDD = examples.map(_.label)
   val numFeatures = examples.take(1)(0).features.size
   println("numFeatures::"+numFeatures)
   val corrType= "pearson"
   println(s"correlation ${corrType} between each label and feature")
   
   var feature = 0

   while(feature < numFeatures){
     val featureRDD = examples.map(line=>line.features(feature))
     val corr = Statistics.corr(labelRDD,featureRDD,corrType)
     println(s"$feature\t$corr")
     feature += 1
     
   }
  
   println()
   context.stop()
 }
}