package com.scala.spark.ml

import org.apache.spark.ml.feature.MinMaxScaler

object MinMaxScalerExample {
  def main(args: Array[String]){
    val sqlContext = UtilsClass.createSQLContext("MinMaxScalerExample")
    
    val dataSet = sqlContext.read.format("libsvm").load(UtilsClass.sourcePath()+"sample_libsvm_data.txt")
    
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scalerfeatures")
    
    // Compute summary statistics and generate MinMaxScalerModel
    val sFeatures = scaler.fit(dataSet)
    
    // rescale each feature to range [min, max].
    sFeatures.transform(dataSet).show()
    
    
    
  }
}