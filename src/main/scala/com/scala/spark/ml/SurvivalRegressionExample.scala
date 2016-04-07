package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.regression.AFTSurvivalRegression

object SurvivalRegressionExample {
  def main(args: Array[String]): Unit = {
    val sqlContext  = UtilsClass.createSQLContext("SurvivalRegressionExample")
    val dataset     = sqlContext.createDataFrame(Seq(
                      (1.218,1.0,Vectors.dense(1.560,-0.605)),
                      (2.949,0.0,Vectors.dense(0.346,2.158)),
                      (3.627,0.0,Vectors.dense(1.380,0.231)),
                      (0.273,1.0,Vectors.dense(0.520,1.151)),
                      (4.199,0.0,Vectors.dense(0.795,-0.226)))).toDF("label","censor","features")
                      
                      
     val quantileProbabilites = Array(0.6,0.3)
     
     val aft        = new AFTSurvivalRegression()
                      .setQuantileProbabilities(quantileProbabilites)
                      .setQuantilesCol("quantiles")
     val model      = aft.fit(dataset)
     
     // Print the coefficients, intercept and scale parameter for AFT survival regression
     println(s"Coefficients: ${model.coefficients} Intercept: " +
     s"${model.intercept} Scale: ${model.scale}")
     model.transform(dataset).show(false)
  }
}