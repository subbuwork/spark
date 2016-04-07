package com.scala.spark.ml

import org.apache.spark.ml.feature.SQLTransformer

object SQLTransformExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("SQLTransformExample")
    
    val df = sqlContext.createDataFrame(Seq((0,1.0,3.0),(2,2.0,5.0))).toDF("id","v1","v2")
    
    val sql = new SQLTransformer().setStatement("select *, (v1+v2) AS v3,(v1*v2) AS v4 from _this_").transform(df).show()
    
  
  
  }
}