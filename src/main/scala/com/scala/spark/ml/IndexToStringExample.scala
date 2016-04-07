package com.scala.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString

object IndexToStringExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("IndexToStringExample")
    
    val df = sqlContext.createDataFrame(Seq(
                                            (0, "a"),
                                            (1, "b"),
                                            (2, "c"),
                                            (3, "a"),
                                            (4, "a"),
                                            (5, "c"))).toDF("id","category")
                                            
    val indexer = new StringIndexer()
                               .setInputCol("category")
                               .setOutputCol("originalCategory")
                               .fit(df).transform(df)
    
    val converter = new IndexToString()
                               .setInputCol("originalCategory")
                               .setOutputCol("baclto")
                               .transform(indexer)
                               .select("backTo").show()
  }
}