package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

object DecisionTreeClassification {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("DecisionTreeClassification")
    
    val dataSet = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer().
                       setInputCol("label").
                       setOutputCol("indexedLabel").fit(dataSet)
    
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().
                         setInputCol("features").
                         setOutputCol("indexedFeatures").
                         setMaxCategories(4).fit(dataSet)
                         
    // Split the data into training and test sets (30% held out for testing)
     val Array(trainingData,testData) = dataSet.randomSplit(Array(0.7,0.3))
     
     // Train a DecisionTree model
     val dr = new DecisionTreeClassifier().
              setLabelCol("indexedLabel").
              setFeaturesCol("indexedFeatures")
              
     // Convert indexed labels back to original labels
     val labelConverter = new IndexToString().
                          setInputCol("prediction").
                          setOutputCol("predictedLabel").
                          setLabels(labelIndexer.labels)         
                         
    // Chain indexers and tree in a Pipeline
    val pipeLine = new Pipeline().setStages(Array(labelIndexer,featureIndexer,dr,labelConverter))
    
    // Train model.  This also runs the indexers.
    val model = pipeLine.fit(trainingData)
    
    // Make predictions.
    val predictions = model.transform(testData)
    
    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)
    
    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator().
                    setLabelCol("indexedLabel").
                    setPredictionCol("prediction").
                    setMetricName("precision")
                    
    val accuracy = evaluator.evaluate(predictions) 
    println("Accuracy = " +accuracy)   
     println("Test Error = " + (1.0 - accuracy)) 
     
     
     val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
     println("Learned classification tree model:\n" + treeModel.toDebugString)                     
    
    
    
  }
}