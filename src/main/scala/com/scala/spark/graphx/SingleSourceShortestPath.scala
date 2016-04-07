package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object SingleSourceShortestPath {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("SingleSourceShortestPath")
    val ctx = new SparkContext(conf)
    
    
    val graph:Graph[Long,Double]= GraphGenerators.logNormalGraph(ctx, numVertices=10).mapEdges(e=>e.attr.doubleValue())
    
    val sourceID:VertexId =15
    val initialGraph = graph.mapVertices((id,_)=>if(id == sourceID)0.0 else Double.PositiveInfinity)
    /*val sssp = initialGraph.pregel(Double.PositiveInfinity)((id,dist,newDist)=>math.min(dist, newDist),triplet=>{
      if(triplet.srcAttr+triplet.attr<triplet.dstAttr){
        Iterator(triplet.dstId, triplet.srcAttr+triplet.attr)
      }else{
        iterator
      }
    }, mergeMsg)*/
    
  }
  
}