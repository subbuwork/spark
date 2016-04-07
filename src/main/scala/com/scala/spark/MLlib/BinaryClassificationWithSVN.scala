package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMModel

object BinaryClassificationWithSVN {
   def main(args:Array[String]){
     
        val conf    = new SparkConf().setMaster("local").setAppName("BinaryClassificationWithSVN")
        val context = new SparkContext(conf)
        
        // lading data 
        val data = MLUtils.loadLibSVMFile(context,"hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
        
        // split data into training(60%) and test(40%)
        
        val splits   = data.randomSplit(Array(0.6,0.4),11L)
        val training = splits(0).cache()
        val test     = splits(1)
        
        // run training algorithm to build model
        
        val numiters = 100
        
        val model = SVMWithSGD.train(training, numiters)
        
        model.clearThreshold()
        
        // Compute raw scores on the test set.
        
        val socreAndLabel = test.map{point =>
          val score = model.predict(point.features)
          (score,point.label)
        }
        
        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(socreAndLabel)
        val auROC   = metrics.areaUnderROC()
        println("Area under ROC::"+auROC)
     // Save and load model
       model.save(context, "myModelPath")
      val sameModel = SVMModel.load(context, "myModelPath")
      println(sameModel)
   }
  
  
}