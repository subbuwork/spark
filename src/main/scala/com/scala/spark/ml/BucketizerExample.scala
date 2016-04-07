package com.scala.spark.ml

import org.apache.spark.ml.feature.Bucketizer

object BucketizerExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("BucketizerExample")
    
    val splits = Array(Double.NegativeInfinity,-0.5,0.0,0.5,Double.PositiveInfinity)
    
    val data = Array(-0.5,-0.3,0.0,0.2)
    
    val dataFrame = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    
    // Transform original data into its bucket index
    val bucketizer = new Bucketizer()
                     .setInputCol("features")
                     .setOutputCol("bucketizerFeature")
                     .setSplits(splits)
                     .transform(dataFrame).show()
  }
}