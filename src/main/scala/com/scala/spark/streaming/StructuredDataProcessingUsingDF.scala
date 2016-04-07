package com.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object StructuredDataProcessingUsingDF {
  def main(args:Array[String])={
    
    val conf = new SparkConf().setMaster("local").setAppName("StructuredDataProcessingUsingDF")
    val context = new SparkContext(conf)
    
    val sqlContext = new SQLContext(context)
    
    val dataFrame = sqlContext.read.json("/home/hduser/Desktop/inputdata/data/people.json")
    
    // show the content for the dataframe
    dataFrame.show()
    
    // show is the schema
    
   dataFrame.printSchema()
    
    //show only names
  dataFrame.select("name").show()
    
    //select every body but increase age by 1
    
   dataFrame.select(dataFrame("name"),dataFrame("age")+1).show()
    
    //select people older than>30
    
    dataFrame.select(dataFrame("age")<30).show()
    
    //Count people by age
    dataFrame.groupBy("age").count().show()
    //filter people age ==30
   dataFrame.filter("age=30").show()
   

    
  }
  
}