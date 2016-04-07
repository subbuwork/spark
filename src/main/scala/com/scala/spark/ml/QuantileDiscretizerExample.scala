package com.scala.spark.ml

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.QuantileDiscretizer

object QuantileDiscretizerExample {
  def main(args:Array[String]){
  val sc = UtilsClass.getSparkContext("QuantileDiscretizerExample")
  val sqlContext = new SQLContext(sc)
  
  
  
  val data = Array((0,18.0),(1,19.0),(2,8.0),(3,5.0),(4,2.2))
  
  import sqlContext.implicits._
  
  val df = sc.parallelize(data).toDF("id","hour")
  
  val qd = new QuantileDiscretizer().setInputCol("hour").setOutputCol("result").setNumBuckets(3).fit(df).transform(df).show()
  }
}