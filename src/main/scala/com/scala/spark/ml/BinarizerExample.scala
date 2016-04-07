package com.scala.spark.ml

import org.apache.spark.ml.feature.Binarizer

object BinarizerExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("BinarizerExample")
    
    val data = Array((0,0.1),(2,0.8),(3,0.5))
    val dataFrame = sqlContext.createDataFrame(data).toDF("label","feature")
    
    val binarizer = new Binarizer().setInputCol("feature").setOutputCol("binarized_features").setThreshold(0.3)
    binarizer.transform(dataFrame).select("binarized_features").collect().foreach(println)
  }
}