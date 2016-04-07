package com.apache.spark.ml.regression

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.regression.RandomForestRegressionModel

object RandomForestTreeRegression {
  def main(args: Array[String]): Unit = {
    val sqlctx = UtilsClass.createSQLContext("RandomForestTreeRegression")
    val dataset = sqlctx.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val featuresIndexer = new VectorIndexer()
                        .setInputCol("features")
                        .setOutputCol("indexedFeatures")
                        .setMaxCategories(4)
                        .fit(dataset)
                        
    val Array(trainingdata,testdata) = dataset.randomSplit(Array(0.7,0.3))
    
    val rftr           = new RandomForestRegressor()
                       .setLabelCol("label")
                       .setFeaturesCol("indexedFeatures")
    // chain tree and indexer
    val pipeline       = new Pipeline().setStages(Array(featuresIndexer,rftr))
    
    // train model
    val model          = pipeline.fit(trainingdata)
    
    // make predictions
    val predictions    = model.transform(testdata)
    
    predictions.select("prediction","label","features").show(10)
    
    val evaluator      = new RegressionEvaluator()
                       .setLabelCol("label")
                       .setPredictionCol("prediction")
                       .setMetricName("rmse")
    val rmse           = evaluator.evaluate(predictions)
    println("Root mean square error:::="+rmse)
    
    val treeModel      = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Tree Model:\n"+treeModel.toDebugString)
    
  }
}