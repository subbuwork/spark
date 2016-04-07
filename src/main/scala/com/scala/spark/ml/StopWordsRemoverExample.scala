package com.scala.spark.ml

import org.apache.spark.ml.feature.StopWordsRemover

object StopWordsRemoverExample {
  def main(args:Array[String]){
   // val sc = UtilsClass.getSparkContext()
    val sql = UtilsClass.createSQLContext("StopWordsRemoverExample")
    
    val data = sql.createDataFrame(Seq(
               (0, Seq("I", "saw", "the", "red", "baloon")),
               (1, Seq("Mary", "had", "a", "little", "lamb")))).toDF("label","raw")
    val remover = new StopWordsRemover()
     remover.setInputCol("raw")
     remover.setOutputCol("result")
     remover.setStopWords(Array("the","a"))
     remover.transform(data).show()
  }
}