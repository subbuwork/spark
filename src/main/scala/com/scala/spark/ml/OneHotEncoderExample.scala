package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder

object OneHotEncoderExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("OneHotEncoderExample")
    val df = sqlContext.createDataFrame(Seq(
                                         (0, "a"),
                                        (1, "b"),
                                        (2, "c"),
                                        (3, "a"),
                                        (4, "a"),
                                        (5, "c"))).toDF("id","category")
                                        
   val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df).transform(df)
   
   val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("encoded").transform(indexer).show()
  }
}