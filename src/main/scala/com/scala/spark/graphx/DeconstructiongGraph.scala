package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object DeconstructiongGraph {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("DeconstructiongGraph")
    val context = new SparkContext(conf)
    
    //creating an RDD for vertices
    val users:RDD[(VertexId,(String,String))] = context.parallelize(Array((3L,("aadhay","stu")),
        (5L,("frank","prof")),(7L,("neelima","postdoc")),(2L,("xyz","prof"))))
        
    val relations:RDD[Edge[String]]=context.parallelize(Array(Edge(3L,7L,"collab"),Edge(5L,3L,"Advisor"),
        Edge(2L,5L,"colleague"),Edge(5L,7L,"PI")))
        
    val defaultuser=("abc","Missing")
    // Constructing graph
    val graph1= Graph(users,relations,defaultuser)
    
    // Deconstructing Graph
   // val graph: Graph[(String, String), String]
    
    val count=graph1.vertices.filter{case(id,(name,pos))=>pos=="postdoc"}.count()
    println(count) 

    val vCount= graph1.vertices.filter{case(id,(name,pro))=> pro=="stu"}.count()
    println(vCount)
    val eCount= graph1.edges.filter(e=>e.srcId>e.dstId).count()
    println(eCount)
    
    val facts:RDD[String]=graph1.triplets.map(triplet=>triplet.srcAttr._1 +"is the"+triplet.attr+"of"+triplet.dstAttr._1)
    facts.collect.foreach(println(_))
  }
}