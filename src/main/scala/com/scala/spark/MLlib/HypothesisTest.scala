package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.stat.test.ChiSqTestResult

object HypothesisTest {
  def main(args:Array[String]){
    
    val conf    = new SparkConf().setMaster("local").setAppName("HypothesisTest")
    val context = new SparkContext(conf)
    
    val vc:Vector = Vectors.dense(Array(1.0,2.0,3.0))
    
    val goodnessOfFittest = Statistics.chiSqTest(vc)
    println(goodnessOfFittest)
    
    val obs:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(context,"hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt").cache()
    
    val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
    var i = 1
     featureTestResults.foreach { result =>
    println(s"Column $i:\n$result")
    i += 1
    }
}
     
}