package com.scala.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.RegexTokenizer

object TokenizerExample {
   
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("TokenizerExample")
    val context = new SparkContext(conf)
    val sql  = new SQLContext(context)
    
    val data = sql.createDataFrame(Seq(
                                 (0, "Hi I heard about Spark"),
                                 (1, "I wish Java could use case classes"),
                                 (2, "Logistic,regression,models,are,neat"))).toDF("label","sentense")
                                 
    val tokenizer = new Tokenizer().setInputCol("sentense").setOutputCol("words")
    val regToken  = new RegexTokenizer()
                     .setInputCol("sentense")
                     .setOutputCol("words")
                     .setPattern("\\W")
    val tokenized = tokenizer.transform(data)
    tokenized.select("label","words").take(3).foreach(println)
    
    println("***************************************************************************")
    
    val regexToken = regToken.transform(data)
    regexToken.select("label","words").take(3).foreach(println)
    
    
  }
  
}