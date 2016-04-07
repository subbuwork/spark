package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object SparseLoadingData {
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("SparseLoadingData")
    val ctx = new SparkContext(conf)
    
    val exampleLoading:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(ctx,"hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
    exampleLoading.cache()
    
    val id =exampleLoading.id
    println(id)
    
    
  }
}