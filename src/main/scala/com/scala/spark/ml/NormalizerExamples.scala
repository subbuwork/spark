package com.scala.spark.ml

import org.apache.spark.ml.feature.Normalizer

object NormalizerExamples {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("NormalizerExamples")
    
    val dataSet = sqlContext.read.format("libsvm").load("hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
    
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normalized").setP(5.0)
    normalizer.transform(dataSet).show()
  }
}