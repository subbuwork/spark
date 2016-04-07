package com.apache.spark.ml.classification

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.GBTClassificationModel

object GBTClassification {
  // Gradient Boosted Tree is popular classification and regression method using ensembles of decision tress
  def main(args: Array[String]): Unit = {
    val sqlcontext = UtilsClass.createSQLContext("GBTClassification")
    
    val dataset    = sqlcontext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val labelindexer = new StringIndexer()
                       .setInputCol("label")
                       .setOutputCol("indexedLabels")
                       .fit(dataset)
                       
    val featureIndexer = new VectorIndexer()
                       .setInputCol("features")
                       .setOutputCol("indexedFeatures")
                       .setMaxCategories(4)
                       .fit(dataset)
     // split data into two parts training data 60% and test data 30%                  
      val Array(trainingdata,testdata) = dataset.randomSplit(Array(0.7,0.3))                 
                       
    val gbt            = new GBTClassifier()
                       .setLabelCol("indexedLabels")
                       .setFeaturesCol("indexedFeatures")
                       .setMaxIter(10)
                       
    val conveter       = new IndexToString()
                       .setInputCol("prediction")
                       .setOutputCol("predictedLabels")
                       .setLabels(labelindexer.labels)
                       
    val  pipeline      = new Pipeline()
                       .setStages(Array(labelindexer,featureIndexer,gbt,conveter))
                       
    // train model
    val model          = pipeline.fit(trainingdata)
    
    // Make predictions
    
    val predictions    = model.transform(testdata)
        predictions.select("predictedLabels","label","features").show(10)
        
    // test error
    val evaluation     = new MulticlassClassificationEvaluator()
                       .setLabelCol("indexedLabels")
                       .setPredictionCol("prediction")
                       .setMetricName("precision")
     val accuracy      = evaluation.evaluate(predictions) 
     println("Test error:="+(1.0-accuracy))
     
     // Tree model
     val treeModel     = model.stages(2).asInstanceOf[GBTClassificationModel]
     println("Tree Model:\n"+treeModel.toDebugString)
                       
                       
                       
  }
}