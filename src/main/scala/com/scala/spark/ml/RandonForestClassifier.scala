package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.RandomForestClassificationModel

object RandonForestClassifier {
  def main(args: Array[String]): Unit = {
    
    val sqlContext = UtilsClass.createSQLContext("RandonForestClassifier")
    
     // Load and parse the data file, converting it to a DataFrame.
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData,testData) = dataset.randomSplit(Array(0.7,0.4))
    
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer().
                       setInputCol("label").  
                       setOutputCol("indexedLabel").
                       fit(dataset)
     // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.                  
    val featureIndexed = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedFeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
     // Train a RandomForest model.                    
    val rf              = new RandomForestClassifier()
                          .setLabelCol("indexedLabel")
                          .setFeaturesCol("indexedFeatures")
                          .setNumTrees(10)
    // Convert indexed labels back to original labels.
                      
   val labelConveter    = new IndexToString()
                          .setInputCol("prediction")
                          .setOutputCol("predictedLabel")
                          .setLabels(labelIndexer.labels)
                          
// Chain indexers and forest in a Pipeline
   val pipeline         = new Pipeline()
                          .setStages(Array(labelIndexer,featureIndexed,rf,labelConveter))
                          
   // train model
   val model            = pipeline.fit(trainingData)
   
   // make predictions
   val predictions      = model.transform(testData)
   
   // Select example rows to display.
   predictions.select("predictedLabel","label","features").show(20)
   
    // Select (prediction, true label) and compute test error
   val evaluate        = new MulticlassClassificationEvaluator()
                         evaluate.setLabelCol("indexedLabel")
                         evaluate.setPredictionCol("prediction")
                         evaluate.setMetricName("precision")
                         
   val accuracy        = evaluate.evaluate(predictions)
   println("Test error =:"+(1.0-accuracy))
   
   val treeModel      = model.stages(2).asInstanceOf[RandomForestClassificationModel]
   println("Learned classification forest model:\n"+treeModel.toDebugString) 
                          
  }
}