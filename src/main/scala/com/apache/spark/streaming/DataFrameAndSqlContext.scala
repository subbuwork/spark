package com.apache.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import com.scala.spark.streaming.Record

object DataFrameAndSqlContext {
  def main(args:Array[String]){
  val sc = UtilsClass.createStreaingContext("DataFrameAndSqlContext")
  
  val dStream = sc.receiverStream(new TestReceiver("localhsot",9999))
  val wordsDStream = dStream.flatMap(_.split(" "))
  
  wordsDStream.foreachRDD ((rdd:RDD[String],time:Time) =>{
    
    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    import sqlContext.implicits._
    
    val df = rdd.map(word=>Record(word)).toDF("word")
    
    df.registerTempTable("words")
    
    val wordsCountDF = sqlContext.sql("select word, count(*) as total from words group by word")
    println(s"========= $time=========")
    wordsCountDF.show()
    
    
    
    
  })
  sc.start()
  sc.awaitTermination()
}
}