package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object ConnectedComponents {
  def main(args:Array[String])={
    
    val conf = new SparkConf().setMaster("local").setAppName("ConnectedComponents")
    val context = new SparkContext(conf)
    
    // loading graph
    var graph = GraphLoader.edgeListFile(context,"hdfs://localhost:9000/user/input/follower.txt")
    graph.cache()
    val cc = graph.connectedComponents().vertices
    
    val users = context.textFile("hdfs://localhost:9000/user/input/users.txt").map{line=>
      val fields = line.split(",")
      (fields(0).toLong,fields(1))
    }
    users.cache()
    
    val ccByUserName = users.join(cc).map{
      case(id,(username,cc))=>(username,cc)
    }
    
    println(ccByUserName.collect().mkString("\n"))
  }
}