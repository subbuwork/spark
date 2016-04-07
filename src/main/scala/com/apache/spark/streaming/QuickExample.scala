package com.apache.spark.streaming

object QuickExample {
  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 5 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val sc = UtilsClass.createStreaingContext("QuickExample")
    
    // Create a DStream that will connect to hostname:port, like localhost:9999
    
    val lines = sc.socketTextStream("localhost",9999)
    
    // split each line into words
    
    val words = lines.flatMap(_.split(" "))
    
    //// Count each word in each batch
    
    val pairs = words.map(word=>(word,1))
    
     val wordsCount=pairs.reduceByKey(_+_)
     wordsCount.print()
     
    sc.start()  // Start the computation
    
    sc.awaitTermination() // Wait for the computation to terminate;
    
  }
}