package com.scala.spark.ml

import org.apache.spark.ml.feature.StandardScaler

object StandardScalerExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("StandardScalerExample")
    val dataFrame = sqlContext.read.format("libsvm").load("hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
    
    val sc = new StandardScaler().setInputCol("features").setOutputCol("feat").setWithStd(true).setWithMean(false)
    
    // Compute summary statistics by fitting the StandardScaler.
    val scModel= sc.fit(dataFrame)
    
   // Normalize each feature to have unit standard deviation.
    scModel.transform(dataFrame).show()
  }
}
