package com.scala.sprk.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CreatingDataSets {
  def main(args:Array[String])={
     val conf = new SparkConf().setAppName("CreatingDatasets").setMaster("local")
    val context= new SparkContext(conf)
    val sqlContext = new SQLContext(context)
     
     import sqlContext.implicits._
   
    val ds1 = Seq(Person("subbu",32)).toDS()
    val people = sqlContext.read.json("/home/hduser/Desktop/inputdata/data/people.json").as[Person]
    //val people1= sqlContext.load("/home/hduser/Desktop/inputdata/data/people.json")
   // people1.show()
     people.show()
  }
}