package com.apache.spark.ml.classification

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import com.scala.spark.ml.RandonForestClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassificationModel

object RandomForestClassification {
  // Random forest if popular family of classification and regression
  
  def main(args: Array[String]): Unit = {
    val sqlcontext = UtilsClass.createSQLContext("RandomForestClassification")
    val dataset    = sqlcontext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val labelIndexer   = new StringIndexer()
                         .setInputCol("label")
                         .setOutputCol("indexedlabel")
                         .fit(dataset)
                         
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedfeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
    val Array(traingdata,testdata) = dataset.randomSplit(Array(0.6,0.3))
    
    val rfc           = new RandomForestClassifier()
                        .setLabelCol("indexedlabel")
                        .setFeaturesCol("indexedfeatures")
                        .setNumTrees(10)
                        
   val converter      = new IndexToString()                     
                         .setInputCol("prediction")
                         .setOutputCol("predictedLabel")
                         .setLabels(labelIndexer.labels)
                         
   val pipeline        = new Pipeline()
                         .setStages(Array(labelIndexer,featureIndexer,rfc,converter))
                         
   // Train Model
   val model           = pipeline.fit(traingdata)
   
   // make predictions 
   val predictions     = model.transform(testdata)
   predictions.select("predictedLabel","label","features").show(10)
   
   val evaluation     = new MulticlassClassificationEvaluator()
                        .setLabelCol("indexedlabel")
                        .setPredictionCol("prediction")
                        .setMetricName("precision")
   val accuracy      = evaluation.evaluate(predictions)
   
   println("Test error:\n"+(1.0-accuracy))
   
   val treemodel     = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Tree model:::"+treemodel.toDebugString)
                         
  }
}