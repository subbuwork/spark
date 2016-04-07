package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions

object LinearLeastSquares {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("LinearLeastSquares")
    val context = new SparkContext(conf)
    
    val data = context.textFile("hdfs://localhost:9000/user/sparkdata/mllib/lspa.data")
    val parseData = data.map{line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    
 // Building the model
 val numIter = 100
 val model   = LinearRegressionWithSGD.train(parseData, numIter)
 
 //RidgeRegressionWithSGD and LassoWithSGD can be used in a similar fashion as LinearRegressionWithSGD.
 
 // Evaluate model on training examples and compute training error
 val valuesAndPreds = parseData.map{line =>
   val predectioins = model.predict(line.features)
   (line.label,predectioins)
      
    }
val MSE = valuesAndPreds.map{case(v,p)=>math.pow((v-p),2)}.mean()
println("training Mean Squared Error = " + MSE)
    
  }
}