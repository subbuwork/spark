package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object JSONDataSets {
  def main(args:Array[String])={
    val cf = new SparkConf().setMaster("local").setAppName("JSONDataSets")
    val ctx = new SparkContext(cf)
    val sqlctx = new SQLContext(ctx)
    
    val result = sqlctx.read.json("hdfs://localhost:9000/user/input/people.json")
    result.foreach(println)
    result.printSchema()
    result.registerTempTable("people")
    
    val teenagers = sqlctx.sql("select * from people where age>=20")
    teenagers.foreach(println)
    val rdd = ctx.parallelize("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val rdd1 = sqlctx.read.json(rdd)
    rdd.foreach(println)
    rdd1.foreach(println)
    
    
  }
}