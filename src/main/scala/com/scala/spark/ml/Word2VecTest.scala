package com.scala.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec

object Word2VecTest {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Word2VecTest")
    val context = new SparkContext(conf)
    
    val sql = new SQLContext(context)
    
    // Input data: Each row is a bag of words from a sentence or document.
    val sentenseData = sql.createDataFrame(Seq(
                          "Hai how are you".split(" "),
                          "How is your job".split(" "),
                          "how is your family".split(" "),
                          "how is your mother".split(" "),
                          "how is your sister".split(" "),
                          "how is your brother").map(Tuple1.apply)).toDF("text")
                          
     // Learn a mapping from words to Vectors.                     
    val w2v = new Word2Vec()
        .setInputCol("text")
        .setOutputCol("result")
        .setVectorSize(3)
        .setMinCount(0)
   
   val model = w2v.fit(sentenseData)
   val result = model.transform(sentenseData)
   result.select("result").take(3).foreach(println)
  }
}