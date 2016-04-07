package com.apache.spark.ml.regression

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

object DecisionTreeRegression {
  def main(args: Array[String]): Unit = {
    val sqlcontext = UtilsClass.createSQLContext("DecisionTreeRegression")
    
    val dataset = sqlcontext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val featureIndexer = new VectorIndexer()
                       .setInputCol("features")
                       .setOutputCol("indexedFeatures")
                       .setMaxCategories(4)
                       .fit(dataset)
                       
    val Array(trainingdata,testdata) = dataset.randomSplit(Array(0.7,0.3))
    
    val dtr            = new DecisionTreeRegressor()
                       .setLabelCol("label")
                       .setFeaturesCol("indexedFeatures")
                       
    // Chain indexer and tree in a Pipeline                   
    val pipeline       = new Pipeline().setStages(Array(featureIndexer,dtr))  
    
   // Train model
   val model           = pipeline.fit(dataset)
   
   // Make prediction
   val predictions     = model.transform(testdata)
   
   predictions.select("prediction","label","features").show(10)
   
   val evaluation      = new RegressionEvaluator()
                       .setLabelCol("label")
                       .setPredictionCol("prediction")
                       .setMetricName("rmse")
                       
   val rmse            = evaluation.evaluate(predictions)                    
   println("Root Mean Squared Error (RMSE) on test data = " + rmse)
   
   val treeModel       = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
   println("Learned regression tree model:\n"+treeModel.toDebugString) 
   
                       
  }
}