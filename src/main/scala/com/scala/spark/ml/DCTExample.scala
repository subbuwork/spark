package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.DCT

object DCTExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("DCTExample")
    
    val data = Array(
        
                    Vectors.dense(0.0, 1.0, -2.0, 3.0),
                    Vectors.dense(-1.0, 2.0, 4.0, -7.0),
                    Vectors.dense(14.0, -2.0, -5.0, 1.0))
    
   val dctDF = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
   
   val dct = new DCT().setInputCol("features").setOutputCol("dctFeatures").setInverse(false)
   val dctFeatures = dct.transform(dctDF)
   dctFeatures.select("dctFeatures").show(3)
   
  }
}