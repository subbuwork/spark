package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object InferringTheSchemaUsingReflection {
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("InferringTheSchemaUsingReflection")
    val context = new SparkContext(conf)
    
    val sqlContext = new SQLContext(context)
    
    val people = context.textFile("hdfs://localhost:9000/user/input/people.txt")
    
    import sqlContext.implicits._
    
    //Create an RDD of Person objects and register it as a table.
    val peopleDF = people.map(_.split(",")).map(p=>Person1(p(0),p(1).trim().toInt)).toDF().registerTempTable("people")
    
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenages = sqlContext.sql("select * from people where age >=13 and age <=19")
    
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenages.map(t =>"Name:"+t(0)).collect().foreach(println)
    
    // or by field name:
    teenages.map(t=>"Name:"+t.getAs[String]("name")).collect().foreach(println)
    
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenages.map(_.getValuesMap[Any](List("name","age"))).collect().foreach(println)
    // OP Map("Name"->"Justin","age"->"13")
  }
}