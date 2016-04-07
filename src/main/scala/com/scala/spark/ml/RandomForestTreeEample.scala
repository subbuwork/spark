package com.scala.spark.ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.RandomForestRegressionModel

object RandomForestTreeEample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("RandomForestTreeEample")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    // splitting data into 70% and 30%
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.3))
    
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedFeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
    // Train a RandomForest model.
    val rfr            = new RandomForestRegressor()
                         .setLabelCol("label")
                         .setFeaturesCol("indexedFeatures")
     
     // Chain indexer and forest in a Pipeline                    
    val pipeline       = new Pipeline().setStages(Array(featureIndexer,rfr)) 
    
    // fit the model
    val model          = pipeline.fit(trainingData)
    
    // make predictions
    val predictions    = model.transform(testData)
    
    predictions.select("prediction","label","features").show(10)
    
    val evaluator      = new RegressionEvaluator()
                         evaluator.setLabelCol("label")
                         evaluator.setPredictionCol("prediction")
                         evaluator.setMetricName("rmse")
                         
     val rmse          = evaluator.evaluate(predictions)
     
     println("Root Mean Squared Error (RMSE) on test data = " + rmse)
     
     val treeModel     = model.stages(1).asInstanceOf[RandomForestRegressionModel]
     println("Learned regression forest model:\n" + treeModel.toDebugString)    
  }
}