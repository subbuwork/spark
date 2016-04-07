package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

object DecisionTreeClassification1 {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("DecisionTreeClassification1")
    // Load data from hdfs which is in libsvm format
    
    val dataset = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val labelIndexer   = new StringIndexer().
                         setInputCol("label").
                         setOutputCol("indexedLabel").fit(dataset)
    
    val featureIndexer = new VectorIndexer().
                         setInputCol("features").
                         setOutputCol("indexedFeatures").
                         setMaxCategories(4).fit(dataset)
                         
    val dt             = new DecisionTreeClassifier().
                         setLabelCol("indexedLabel").
                         setFeaturesCol("indexedFeatures")
                         
    val labelConverter = new IndexToString().
                         setInputCol("prediction").
                         setOutputCol("predictedLabel").
                         setLabels(labelIndexer.labels)
  
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.3))
    
    val pipeline = new Pipeline().setStages(Array(labelIndexer,featureIndexer,dt,labelConverter))
    
    val model    = pipeline.fit(trainingData)
    
    val predictions  = model.transform(testData)
    
    predictions.select("predictedLabel", "label","features").show(10)
    
    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator().
                    setLabelCol("indexedLabel").
                    setPredictionCol("prediction").
                    setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))
    
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
  
  }
}