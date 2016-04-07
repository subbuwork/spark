package com.scala.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary


object LogicsticRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    
    val sqlCtx = UtilsClass.createSQLContext("LinearRegressionExample")
    // Load training data
    val data = sqlCtx.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    // Fit the model
    val lr = new LogisticRegression().setElasticNetParam(0.8).setRegParam(0.1).setMaxIter(10).fit(data)
    
    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lr.coefficients} Intercept: ${lr.intercept}")
    
    
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
// example
    
    val summary = lr.summary
    
    // Obtain the objective per iteration.
    
    val objectiveHistory = summary.objectiveHistory
    
    objectiveHistory.foreach(loss=>println(loss))
    
    
    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    
    val binarySummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]
    
    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    binarySummary.roc.show()
    println(binarySummary.areaUnderROC)
    
    // Set the model threshold to maximize F-Measure
    
    //val fMeasure  = binarySummary.fMeasureByThreshold
    
    //val maxFMeasure = fMeasure.select(max("F-Measure")).head.getDouble(0)  
  }
}