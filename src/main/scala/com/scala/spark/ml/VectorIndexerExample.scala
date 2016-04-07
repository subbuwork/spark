package com.scala.spark.ml

import org.apache.spark.ml.feature.VectorIndexer

object VectorIndexerExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("VectorIndexerExample")
    val data= sqlContext.read.format("libsvm").load("hdfs://localhost:9000/user/sparkdata/mllib/sample_libsvm_data.txt")
    
    val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(10)
                  
    val indexerModel = indexer.fit(data)
    
    val categoricalFeatures:Set[Int] = indexerModel.categoryMaps.keys.toSet
    
    println(s"Chose ${categoricalFeatures.size} categorical features: " +categoricalFeatures.mkString(", "))
    
    // Create new column "indexed" with categorical values transformed to indices
    
    indexerModel.transform(data).show()
    
  }
}