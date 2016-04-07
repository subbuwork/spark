package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.PartitionStrategy

object TriangleCutting {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("TriangleCount")
    val context = new SparkContext(conf)
    
    // Loading graph
    val graph = GraphLoader.edgeListFile(context,"hdfs://localhost:9000/user/input/follower.txt",true).partitionBy(PartitionStrategy.RandomVertexCut)
    
    graph.cache()
    
    val tc = graph.triangleCount().vertices
    
    // join the triangle count with user names
    
    val users = context.textFile("hdfs://localhost:9000/user/input/users.txt").map{line=>
      val fields = line.split(",")
      (fields(0).toLong,fields(1))
    }
    users.cache()
   val tricountByusername = users.join(tc).map{
     case(id,(username,tc))=>(username,tc)
   }
   
   println(tricountByusername.collect().mkString("\n"))
  }
}