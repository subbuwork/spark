package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField

object ProgrammaticallySpecifyingTheSchema {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("ProgrammaticallySpecifyingTheSchema").setMaster("local")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    
    // creating RDD
    val peopleRDD = context.textFile("hdfs://localhost:9000/user/input/people.txt")
    
    val schemaString="name age"
    
    // Importing sql ROW
    import org.apache.spark.sql.Row
    
    // importing Spark sql data types
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    
    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(" ").map(fieldName=>StructField(fieldName,StringType,true)))
    
    // Convert records of the RDD (people) to Rows.
    val rowRDD = peopleRDD.map(_.split(",")).map(row =>Row(row(0),row(1).trim()))
    
    // Apply the schema to the RDD.
    val peopleDF = sqlContext.createDataFrame(rowRDD,schema)
    
    // Register the DataFrames as a table.
    peopleDF.registerTempTable("people")
    
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val queryresult=sqlContext.sql("select * from people")
    
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    queryresult.map(t=>"Name:"+t(0)).collect().foreach(println)    
    
  }
}