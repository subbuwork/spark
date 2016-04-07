package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object PageRank {
  def main(args:Array[String]){
  val conf = new SparkConf().setMaster("local").setAppName("PageRank")
  val context = new SparkContext(conf)
  
  val graph = GraphLoader.edgeListFile(context,"hdfs://localhost:9000/user/input/follower.txt")
  graph.cache()
  
  // Run page rank
  
  val rank = graph.pageRank(0.0001).vertices
  
  // join the user with with ranks

  val users = context.textFile("hdfs://localhost:9000/user/input/users.txt").map{line=>
    val fields = line.split(",")
    (fields(0).toLong,fields(1))
  }
  users.cache()
  
  val ranksByUsername =users.join(rank).map{
    case(id,(username,rank))=>(username,rank)
    
  }
  println(ranksByUsername.collect().mkString("\n"))
  }
}