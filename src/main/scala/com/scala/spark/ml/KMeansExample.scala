package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("KMeansExample")
    
    val dataset = sqlContext.createDataFrame(Seq(
                  (1,Vectors.dense(0.0,0.0,0.0)),
                  (2,Vectors.dense(0.1,0.1,0.1)),
                  (3,Vectors.dense(0.2,0.2,0.2)),
                  (4,Vectors.dense(9.0,9.0,9.0)),
                  (5,Vectors.dense(9.1,9.1,9.1)),
                  (6,Vectors.dense(9.2,9.2,9.2)))).toDF("id","features")
                  
  // trains a k-means model
                  
  val kmodel  = new KMeans()
                .setK(2)
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
  // Fit the kmean model
  val model   = kmodel.fit(dataset)
  
  // shows results
  println("Final centers::::")
  model.clusterCenters.foreach(println)
                
  
  }
}