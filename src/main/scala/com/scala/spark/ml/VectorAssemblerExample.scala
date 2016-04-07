package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler

object VectorAssemblerExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("VectorAssemblerExample")
    
    val df = sqlContext.createDataFrame(Seq(
                       (0,18,1.0,Vectors.dense(0.0,10.0,0.5),1.0)))
                       .toDF("id","hours","mobile","userFeatures","clicked")
                       
                       
    val vs = new VectorAssembler().
             setInputCols(Array("hours","mobile","userFeatures")).
             setOutputCol("features").transform(df).show()                   
  }
}