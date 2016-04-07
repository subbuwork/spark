package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object InferringTheSchemaUsingReflection1 {
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("InferringTheSchemaUsingReflection1")
    val context = new SparkContext(conf)
    
    val sqlContext = new SQLContext(context)
    
    
    // Reading file from HDFS file system
    val peopleRDD = context.textFile("hdfs://localhost:9000/user/input/people.txt",1)
    
    import sqlContext.implicits._
    
    // creating RDD of person objects and registering it as table
    
    val peopleTable = peopleRDD.map(_.split(",")).map(p=>Person(p(0),p(1).trim().toInt)).toDF().registerTempTable("people")
    
    // displaying all records in the table
    sqlContext.sql("select * from people").foreach(println)
    
    // displaying teenagers in the table
    sqlContext.sql("select name,age from people where age<=19").foreach(println)
    
    // Displaying elders in the table
    val teenagers=sqlContext.sql("select name,age from people where age>=20")
    
    // Displaying records based on index
    teenagers.map(i=>i(0)).collect().foreach(println)
    
    
    // or by field name:
    teenagers.map(t=>t.getAs[String]("name")).collect().foreach(println)
    
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagers.map(_.getValuesMap[String](List("name","age")))
    
    
    
    
    
    
  }
}