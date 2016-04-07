package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.GBTClassificationModel

object GradientBoostedTreeClassifier {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("GradientBoostedTreeClassifier")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.3))
    
    val labelIndexer = new StringIndexer()
                       .setInputCol("label")
                       .setOutputCol("indexedLabel")
                       .fit(dataset)
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedFeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
    val gbtc           = new GBTClassifier()
                         .setLabelCol("indexedLabel")
                         .setFeaturesCol("indexedFeatures")
                         .setMaxIter(10)
                         
   val labelConveter   = new IndexToString()
                         .setInputCol("prediction")
                         .setOutputCol("indexedPrediction")
                         .setLabels(labelIndexer.labels)
                         
   val pipeline        = new Pipeline().setStages(Array(labelIndexer,featureIndexer,gbtc,labelConveter))
   
   // fid the model
   val model           = pipeline.fit(trainingData)
   
   // make prediction
   val predictions     = model.transform(testData)
   
   // select prediction and features
   
   predictions.select("indexedPrediction","label","features").show(10)
   
   // Test error
   val evaluator = new MulticlassClassificationEvaluator()
                   evaluator.setLabelCol("indexedLabel")
                   evaluator.setPredictionCol("prediction")
                   evaluator.setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("test error::="+(1.0-accuracy))
    
    val treeModel = model.stages(2).asInstanceOf[GBTClassificationModel]
    println("Tree model:::="+treeModel.toDebugString)
    
    
  }
}