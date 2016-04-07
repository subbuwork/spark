package com.scala.sprk.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
object CreatingDataFrames {
  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("CreatingDataFrames").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    // reading file from HDFS
    //val df = sqlContext.read.json("hdfs://localhost:9000/user/input/people.json")
    
    // reading file from local file system
    val df = sqlContext.read.json("/home/hduser/Desktop/inputdata/data/people.json")
    //import sqlContext.implicits._
    
    // show content of dataframe
    df.show()
    sc.stop()
  }
}