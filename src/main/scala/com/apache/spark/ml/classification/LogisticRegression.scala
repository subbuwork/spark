package com.apache.spark.ml.classification

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.classification.LogisticRegression

object LogisticRegression1 {
  //Logistic regression is a popular method to predict a binary response. 
  //It is a special case of Generalized Linear models that predicts the probability of the outcome
  
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("LogisticRegression")
    val data       = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val lr = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    
    // fit the model
    val lrModel = lr.fit(data)
    
    // Print the coefficients and intercept for logistic regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
  }
}