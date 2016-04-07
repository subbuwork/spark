package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer

object StringIndexedExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("StringIndexedExample") 
    
    val df = sqlContext.createDataFrame(Seq(
               (0, "a"), (1, "b"), (2, "c"), (3, "a"),
               (4, "a"), (5, "c"))).toDF("id","category")
               
    val si = new StringIndexer()
             .setInputCol("category")
             .setOutputCol("categoryIndex")
             .fit(df).transform(df).show()
    
  }
}