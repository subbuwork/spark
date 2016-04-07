package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.ChiSqSelector

object ChiSquareSelectorExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("ChiSquareSelectorExample")
    val dataFrame = sqlContext.createDataFrame(Seq(
        
                  (7,Vectors.dense(0.0,0.0,18.0,1.0),1.0),
                  (8,Vectors.dense(0.0,1.0,12.0,0.0),0.0),
                  (9,Vectors.dense(1.0,0.0,15.0,0.1),0.0))).toDF("id","features","clicked")
  
           val selector = new ChiSqSelector()
                          .setFeaturesCol("features")
                          .setLabelCol("clicked")
                          .setNumTopFeatures(1)
                          .setOutputCol("result").fit(dataFrame).transform(dataFrame).show()
  }
}