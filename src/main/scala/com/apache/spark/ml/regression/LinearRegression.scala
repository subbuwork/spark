package com.apache.spark.ml.regression

import com.scala.spark.ml.UtilsClass
import org.apache.spark.ml.regression.LinearRegression


object LinearRegression1 {
  def main(args: Array[String]): Unit = {
    val sqlcontext = UtilsClass.createSQLContext("LinearRegression1")
    val dataset    = sqlcontext.read.format("libsvm").load(UtilsClass.sourcePath()+"linear_regression_data.txt")
    
    val lr = new LinearRegression()
             .setMaxIter(100)
             .setElasticNetParam(0.8)
             .setRegParam(0.3)
             
             
   val lrModel = lr.fit(dataset)
   
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