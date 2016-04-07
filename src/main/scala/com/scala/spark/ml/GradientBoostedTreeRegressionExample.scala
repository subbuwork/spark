package com.scala.spark.ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.ml.regression.GBTRegressionModel

object GradientBoostedTreeRegressionExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("GradientBoostedTreeRegressionExample")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.3))
    
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedFeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
    // train GBT tree model
                         
   val gbt             = new GBTRegressor()
                         .setLabelCol("label")
                         .setFeaturesCol("indexedFeatures")
                         
  // chain indexer and GBT tree model
  val pipeline        = new Pipeline()
                        .setStages(Array(featureIndexer,gbt))
                        
  // Fit the model
  val model           = pipeline.fit(trainingData)
  
  // Make prediction
  val predictions      = model.transform(testData)
  
  // select columns
  
  predictions.select("prediction","label","features").show(10)
  
  // Test error
  
  val evaluator      = new RegressionEvaluator()
                       evaluator.setLabelCol("label")
                       evaluator.setPredictionCol("prediction")
                       evaluator.setMetricName("rmse")
 val rmse            = evaluator.evaluate(predictions)
 
 println("RMSE:::="+rmse)
 
 val gbtModel       = model.stages(1).asInstanceOf[GBTRegressionModel]
 println("Learned regression GBT model:\n" + gbtModel.toDebugString)   
                         
  }
}