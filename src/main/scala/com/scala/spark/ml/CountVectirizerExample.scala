package com.scala.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.CountVectorizerModel

object CountVectirizerExample {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("CountVectirizerExample")
    val context = new SparkContext(conf)
    val sql  = new SQLContext(context)
    
    val data = sql.createDataFrame(Seq(
                                      (0,Array("a","b","c","d")),
                                      (1,Array("e","f","g")),
                                      (0,Array("e","f","g","h","i","k")))).toDF("id","words")
    val cv:CountVectorizerModel = new CountVectorizer()
             .setInputCol("words")
             .setOutputCol("result")
             .setVocabSize(3)
             .setMinDF(2).fit(data)
             
             
      // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")
      
    cv.transform(data).select("result").show()
  }
}