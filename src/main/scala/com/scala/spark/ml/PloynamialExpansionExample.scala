package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.PolynomialExpansion

object PloynamialExpansionExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("PloynamialExpansionExample")
    
    val data = Array(
                    Vectors.dense(Array(-2.0,2.3)),
                    Vectors.dense(Array(0.0,0.0)),
                    Vectors.dense(Array(0.6,-1.1)))
                    
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val ps = new PolynomialExpansion().setInputCol("features").setOutputCol("pfeatures").setDegree(3)
    val polyDF = ps.transform(df)
    polyDF.select("pfeatures").take(3).foreach(println)
    
  }
}