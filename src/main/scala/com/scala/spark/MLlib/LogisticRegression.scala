package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.LogisticRegressionModel

object LogisticRegression {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("LogisticRegression")
    val context = new SparkContext(conf)
    
    val data = MLUtils.loadLibSVMFile(context,"hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
    
    // splitting data training(60%) and text(40%)
    
    val splits   = data.randomSplit(Array(0.6,0.4),11L)
    val training = splits(0).cache()
    val test     = splits(1)
    
    // run training alg on  training to build model
    val model    = new LogisticRegressionWithLBFGS()
                   .setNumClasses(5)
                   .run(training)
                   
    // Compute scores on raw test set
    val predictionAndLabels = test.map{case LabeledPoint(label,feature) =>
      val prediction        = model.predict(feature)
      (prediction,label)
    }
    
    // Get evaluation metrics
    val metrics   = new MulticlassMetrics(predictionAndLabels)
    val precesion = metrics.precision
    println(precesion)
    
    // save and load model
    
    model.save(context,"myModelpath")
    val sameModel = LogisticRegressionModel.load(context, "myModelPath")
    
  }
}