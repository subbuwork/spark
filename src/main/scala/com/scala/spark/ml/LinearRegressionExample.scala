package com.scala.spark.ml

import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("LinearRegressionExample")
    val dataset    = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"linear_regression_data.txt")
    
    val lr = new LinearRegression().setMaxIter(100).setElasticNetParam(0.8).setRegParam(0.3)
    
    // fit the model
    
    val lrModel = lr.fit(dataset)
    
    // Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Summarize the model over the training set and print out some metrics
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
  }
}