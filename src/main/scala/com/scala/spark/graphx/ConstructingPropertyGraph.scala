package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD

object ConstructingPropertyGraph {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setMaster("local").setAppName("ConstructingPropertyGraph")
    val context = new SparkContext(conf)
    
    // create an RDD for vertices
    
    val users:RDD[(VertexId,(String,String))] = context.parallelize(Array((3L, ("rxin", "student")),
                       (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), 
                       (2L, ("istoica", "prof"))))
    // creating an RDD for Edges
    val relationships:RDD[Edge[String]] =context.parallelize(Array(Edge(3L,7L,"collab"),
        Edge(5L,3L,"advisor"),Edge(2L,5L,"colleague"),Edge(5L,7L,"PI"))) 
        
     // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")   
    
    // Build graph
    val graph = Graph(users,relationships,defaultUser)
    
    val value =graph.vertices.first()
    println(value)
    
    val inDegree:VertexRDD[Int] = graph.inDegrees
    val outDegree:VertexRDD[Int] = graph.outDegrees
    
    println("in degree::"+inDegree+":: out degree::"+outDegree)
  }

}