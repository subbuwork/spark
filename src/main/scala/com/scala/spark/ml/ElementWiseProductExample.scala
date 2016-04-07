package com.scala.spark.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.ElementwiseProduct

object ElementWiseProductExample {
  def main(args: Array[String]): Unit = {
    val sqlContext = UtilsClass.createSQLContext("ElementWiseProductExample")
    val dataSet = sqlContext.createDataFrame(Seq(
        
                  ("a", Vectors.dense(1.0,2.0,3.0)),
                  ("b",Vectors.dense(4.0,5.0,6.0)))).toDF("id","vector")
    val tranfarmingVector = Vectors.dense(0.0,1.0,2.0)
    
    val elementWiseProduct = new ElementwiseProduct()
                            .setInputCol("vector")
                            .setOutputCol("vectors")
                            .setScalingVec(tranfarmingVector).transform(dataSet).show()
    
    
  }
}