package com.scala.spark.graphx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader

object ConstructingGraph {
  def main(args:Array[String])={
    val conf = new SparkConf().setMaster("local").setAppName("ConstructingGraph")
    val context = new SparkContext(conf)
    context.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJISMAD7S5X2RV2IA")
    context.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
    
    // Load my user data and parse into tuples of user id and attribute list
    val users = (context.textFile("s3n://subbu-spark/users.txt").map{line=>
                        val fields=line.split(",")
                        (fields.head.toLong,fields.tail)
                                                               })
        users.cache()                                                       
    // Parse the edge data which is already in userId -> userId format
    val followers = GraphLoader.edgeListFile(context,"s3n://subbu-spark/follower.txt")
    followers.cache()
    
    val graph = followers.outerJoinVertices(users){
      case(uid,deg,Some(attrList))=>attrList
      case(uid,deg,None)=>Array.empty[String]
    }
    
    val subGraph = graph.subgraph(vpred = (vid,attr) => attr.size == 2)
    val pageRankGraph = subGraph.pageRank(0.001)
    
   val userinfoWithPageRank = subGraph.outerJoinVertices(pageRankGraph.vertices){
      case (uid,attrList,Some(pro))=>(pro,attrList.toList)
      case(uid,attrList,None)=>(0.0,attrList.toList)
    }
    
    println(userinfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    
  }
}