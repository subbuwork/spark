package com.scala.spark.ml

import org.apache.spark.ml.feature.NGram

object NGramExample {
  def main(args:Array[String]){
    val sqlContext = UtilsClass.createSQLContext("NGramExample")
    val data = sqlContext.createDataFrame(Seq(
     (0, Array("Hi", "I", "heard", "about", "Spark")),
     (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
     (2, Array("Logistic", "regression", "models", "are", "neat")))).toDF("label","words")
  val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
  val ngramFrame = ngram.transform(data)
  ngramFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
  
  }
}