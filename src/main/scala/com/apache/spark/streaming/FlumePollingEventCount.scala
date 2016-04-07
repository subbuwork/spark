package com.apache.spark.streaming

import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.flume._
object FlumePollingEventCount {
 def main(args: Array[String]): Unit = {
   val ssc = UtilsClass.createStreaingContext("FlumePollingEventCount")
   
   val batchInterval = Milliseconds(200)
   
   // Create a flume stream that polls the Spark Sink running in a Flume agent
   
   val stream = FlumeUtils.createPollingStream(ssc, "localhost",9999)
   
   // Print out the count of events received from this server in each batch
   stream.count().map(cnt => "received"+cnt+" flume events").print()
   
   ssc.start()
   ssc.awaitTermination()
 } 
}