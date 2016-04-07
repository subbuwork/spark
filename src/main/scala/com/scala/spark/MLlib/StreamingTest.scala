package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.mllib.stat.test.BinarySample
import org.apache.spark.mllib.stat.test.StreamingTest

object StreamingTest1 {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("StreamingTest")
    //val ctx  = new SparkContext(conf)
    val ssc = new StreamingContext(conf,Seconds(5))
   /* ssc.checkpoint({
      val dir = Utils.
      dir.toString
    })*/
    
    
    val data = ssc.textFileStream("hdfs://localhost:9000/user/input/sparkdata/mllib/Streaming.txt").map(line=>line.split(",")
        match{
              case Array(label,value)=> BinarySample(label.toBoolean,value.toDouble)})
              
         val streamingTest = new StreamingTest()
         streamingTest.setPeacePeriod(0)
         streamingTest.setWindowSize(0)
         streamingTest.setTestMethod("welch")
    
         val out = streamingTest.registerStream(data)
    
   // out.print()
    out.foreachRDD(rdd=>{
    val pi=rdd.map(v=>v.pValue)
    println(pi)
    
    }  )
    
    println(out.print())
    
    ssc.start()
    //ssc.awaitTermination()
  }
}