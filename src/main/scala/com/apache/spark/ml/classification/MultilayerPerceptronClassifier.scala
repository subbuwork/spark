package com.apache.spark.ml.classification

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object MultilayerPerceptronClassifier1 {
  /*Multilayer perceptron classifer is a classifier based on Feedfarward artificial Neural network
   MLPC consist of multiple layers, each layer tightly connected with next layer
   nodes in the input layer represents input data, other nodes maps input data to the output by performing
   linear combination of the inputs with the node's weights w and bias b and applying an activation function*/
  def main(args: Array[String]): Unit = {
    val sqlcontext = UtilsClass.createSQLContext("MultilayerPerceptronClassifier")
    
    val dataset    = sqlcontext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_multiclass_classification.txt")
    
    val splits     = dataset.randomSplit(Array(0.7,0.3),seed=1234L)
    val train      = splits(0)
    val test       = splits(1)
    
    /* specify layers for the neural network:
    input layer of size 4 (features), two intermediate of size 5 and 4
    and output of size 3 (classes)*/
    val layers = Array[Int](4,5,4,3)
    
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
                  .setLayers(layers)
                  .setBlockSize(128)
                  .setSeed(1234L)
                  .setMaxIter(100)
                  
    // train the model
    val model   = trainer.fit(train)
    
    // // compute precision on the test set
    val result  = model.transform(test)
    val predictionAndLabel = result.select("prediction","label")
    val evaluator = new MulticlassClassificationEvaluator()
                    .setMetricName("precision")
    println("Precision:::="+evaluator.evaluate(predictionAndLabel))             
  }
  
}