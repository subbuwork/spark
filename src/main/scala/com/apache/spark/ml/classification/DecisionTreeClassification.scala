package com.apache.spark.ml.classification

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

object DecisionTreeClassification {
  // Decision trees are popular methods of classification and regression
  
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("DecisionTreeClassification")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val labelIndexer = new StringIndexer()
                       .setInputCol("label")
                       .setOutputCol("indexedlabel")
                       .fit(dataset)
                       
    val featureIndexer = new VectorIndexer()
                         .setInputCol("features")
                         .setOutputCol("indexedfeatures")
                         .setMaxCategories(4)
                         .fit(dataset)
                         
    val Array(trainingset,testset) =  dataset.randomSplit(Array(0.6,0.3))
    
    val dtc            = new DecisionTreeClassifier()
                         .setLabelCol("indexedlabel")
                         .setFeaturesCol("indexedfeatures")
                         
    val labelconverter = new IndexToString()
                         .setInputCol("prediction")
                         .setOutputCol("predictedLabel")
                         .setLabels(labelIndexer.labels)
                         
    val pipeline        = new Pipeline()
                          .setStages(Array(labelIndexer,featureIndexer,dtc,labelconverter))
      
   // Train Model                       
    val model           = pipeline.fit(trainingset)
    
    // make predictions
    val predictions     = model.transform(testset)
        predictions.select("predictedLabel", "label","features").show(10)
        
        
    val evaluator       = new MulticlassClassificationEvaluator()
                          .setLabelCol("indexedlabel")
                          .setPredictionCol("prediction")
                          .setMetricName("precision")
   val accuracy         = evaluator.evaluate(predictions)
   
   println("Test error:="+(1.0-accuracy))
   
   val treeModel        = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification model::\n"+treeModel.toDebugString)
                      
  }
}