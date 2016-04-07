package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField

object ProgrammaticallySpecifyingTheSchema1 {
  
  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("ProgrammaticallySpecifyingTheSchema1").setMaster("local")
    val context = new SparkContext(conf)
    
    val sqlContext = new SQLContext(context)
    
    val people =context.textFile("hdfs://localhost:9000/user/input/people.txt",1)
    
    val schemaString = "name age"
    
    import org.apache.spark.sql.types.{StructType,StructField,StringType}
    
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,true)))
    
    import org.apache.spark.sql.Row
    val pRow = people.map(_.split(",")).map(row => Row(row(0),row(1)))
    
    sqlContext.createDataFrame(pRow, schema).registerTempTable("people")
    
    val people1= sqlContext.sql("select * from people")    
    
    sqlContext.sql("select name from people where age>=20").foreach(println)
    
    people1.map(f =>"Name:"+f(0)).collect().foreach(println)
    
    people1.map(_.getValuesMap[Any](List("name","age"))).collect().foreach(println)
    
    
    
  }
  
}