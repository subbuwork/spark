package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy

object CreatingGraphUsingEdgeTuples {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("CreatingGraphUsingEdgeTuples")
    val context = new SparkContext(conf)
    
    val userTuple = context.textFile("hdfs://localhost:9000/user/input/users.txt").map{line=>
      val fileds = line.split(",")
      (fileds(0),1)
    }
    
    //val tupleGraph = Graph.fromEdgeTuples(userTuple,PartitionStrategy.RandomVertexCut)
  }
}