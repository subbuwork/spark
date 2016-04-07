package com.scala.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.{Tokenizer,IDF,HashingTF}

object TFIDF {
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("TFIDF")
    val ctx  = new SparkContext(conf)
    val sqlCtx = new SQLContext(ctx)
    
    val data = sqlCtx.createDataFrame(Seq(
                                     (0, "Hi I heard about Spark"),
                                     (0, "I wish Java could use case classes"),
                                     (1, "Logistic regression models are neat"))).toDF("label","sentense")
                                     
    val tokenizer = new Tokenizer().setInputCol("sentense").setOutputCol("words")
    val wordsData = tokenizer.transform(data)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    val featurizedData = hashingTF.transform(wordsData)
    
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaleData = idfModel.transform(featurizedData)
    rescaleData.select("features","label").take(3).foreach(println)
  }
}