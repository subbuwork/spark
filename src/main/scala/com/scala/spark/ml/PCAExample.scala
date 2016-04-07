package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.PCA

object PCAExample {
  def main(args:Array[String]){
    val sqlcontext = UtilsClass.createSQLContext("PCAExample")
    val data = Array(
                    Vectors.sparse(5,Seq((1,1.0),(3,(2.0)))),
                    Vectors.dense(Array(2.0,0.0,3.0,5.0,2.0)),
                    Vectors.dense(Array(4.0,3.0,2.0,1.0,9.0)))
                    
    val dataFrame = sqlcontext.createDataFrame(data.map(Tuple1.apply)).toDF("feature")
    val pca = new PCA().setInputCol("feature").setOutputCol("pcaFeatures").setK(3).fit(dataFrame)
    pca.transform(dataFrame).select("pcaFeatures").show()
  }
}