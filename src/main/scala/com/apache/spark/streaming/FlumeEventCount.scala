package com.apache.spark.streaming

import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel

object FlumeEventCount {
  def main(args: Array[String]): Unit = {
    val ssc = UtilsClass.createStreaingContext("FlumeEventCount")
    
     val stream = FlumeUtils.createStream(ssc, "localhost", 44444,StorageLevel.MEMORY_ONLY_SER_2)
     stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}