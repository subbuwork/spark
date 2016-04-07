package com.scala.spark.ml

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MultilayerPerciptionClassifier1 {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("MultilayerPerciptionClassifier1")
    val dataset = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_multiclass_classification.txt")
    
    val splits = dataset.randomSplit(Array(0.6,0.4),seed = 1234L)
    
    val trainingData = splits(0)
    val testDtaa     = splits(1)
    
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    
    val layers = Array[Int](4,5,4,3)
    
    // create the trainer and set its parameters
    
    val trainer = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(100)
                
    
    // train the model
    val model   = trainer.fit(trainingData)
    
    // compute precision on the test set
    
    val perciption = model.transform(testDtaa)
    
    val predictionAndLabel = perciption.select("prediction","label")
    
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("precision")
    println("perciption:::"+evaluator.evaluate(predictionAndLabel))
                      
  }
}