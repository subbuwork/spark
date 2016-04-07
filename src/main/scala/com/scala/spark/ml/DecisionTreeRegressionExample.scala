package com.scala.spark.ml

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

object DecisionTreeRegressionExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("DecisionTreeRegressionExample")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.3))
    
    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedFeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
     // Train a DecisionTree model.                    
    val dtr            = new DecisionTreeRegressor()
                         .setLabelCol("label")
                         .setFeaturesCol("indexedFeatures")
                         
    // Chain indexer and tree in a Pipeline                     
   val pipeline        = new Pipeline()
                         .setStages(Array(featureIndexer,dtr))
                         
    // Train model.  This also runs the indexer.                     
   val model           = pipeline.fit(trainingData)
   
   // make  prediction
   val predictions     = model.transform(testData)
    
   // Select example rows to display.
   predictions.select("prediction", "label", "features").show(5)
   
   // Select (prediction, true label) and compute test error
   
   val evaluator    = new RegressionEvaluator()
                      evaluator.setLabelCol("label")
                      evaluator.setPredictionCol("prediction")
                      evaluator.setMetricName("rmse")
                      
   val rmse         = evaluator.evaluate(predictions)
   println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
  println("Learned regression tree model:\n" + treeModel.toDebugString)
   
   
  }
}