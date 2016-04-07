package com.apache.spark.ml.regression

import com.scala.spark.ml.UtilsClass
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.regression.AFTSurvivalRegression

object SurvivalRegression {
  def main(args: Array[String]): Unit = {
    val sqlctx = UtilsClass.createSQLContext("SurvivalRegression")
    val data   = sqlctx.createDataFrame(Seq((1.218,1.0,Vectors.dense(1.560,-0.605))
                                       ,(2.949,0.0,Vectors.dense(0.346,2.158))
                                       ,(3.627,0.0,Vectors.dense(1.380,0.231))
                                       ,(0.273,1.0,Vectors.dense(0.520,1.151))
                                       ,(4.199,0.0,Vectors.dense(0.795,-0.226)))).toDF("label","censor","features")
                                       
    val quantileProbabilites = Array(0.3,0.6)
    
    val aft = new AFTSurvivalRegression()
            .setQuantileProbabilities(quantileProbabilites)
            .setQuantilesCol("quantiles")
            
    val model = aft.fit(data)
    
    // Print the coefficients, intercept and scale parameter for AFT survival regression
    println(s"Coefficients: ${model.coefficients} Intercept: " +
    s"${model.intercept} Scale: ${model.scale}")
    model.transform(data).show(false)
  }
}