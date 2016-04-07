package com.scala.sprk.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object CreatingDatasets {
  
  def main(args:Array[String])={
    
    val conf = new SparkConf().setAppName("CreatingDatasets").setMaster("local")
    val context= new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    
    import sqlContext.implicits._
    
    val ds = Seq(1,2,3,4,5,6).toDS()
    ds.map(_+1).collect()
   // ds.foreach(i=>i+1)
    ds.show()
 
  }
}