package com.apache.spark.ml.regression

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressionModel

object GradientBoostedTreeRegression {
  def main(args: Array[String]): Unit = {
    val sqlctx  = UtilsClass.createSQLContext("GradientBoostedTreeRegression")
    val dataset = sqlctx.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val featureIndexer = new VectorIndexer()
                       .setInputCol("features")
                       .setOutputCol("indexedFeatures")
                       .setMaxCategories(10)
                       .fit(dataset)
    val Array(trainingdata,testdata) = dataset.randomSplit(Array(0.7,0.3))
    
    val gbtr           = new GBTRegressor()
                       .setLabelCol("label")
                       .setFeaturesCol("indexedFeatures")
                       
    // chain tree and indexer 
    val pipeline       = new Pipeline().setStages(Array(featureIndexer,gbtr))
    
    // train model
    val model          = pipeline.fit(trainingdata)
    
    // make predictions
    val predictions    = model.transform(testdata)
    predictions.select("prediction","label","features")
    
    val evaluations    = new RegressionEvaluator()
                       .setLabelCol("label")
                       .setPredictionCol("prediction")
                       .setMetricName("rmse")
   val rmse            = evaluations.evaluate(predictions)
   println("RMSE:::="+rmse)
   
   val gbtrModel       = model.stages(1).asInstanceOf[GBTRegressionModel]
   println("Model:::\n"+gbtrModel.toDebugString)  
  
  
  }
}