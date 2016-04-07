package com.apache.spark.streaming

import com.apache.spark.streaming.UtilsClass

object ForeachRDD {
  def main(args: Array[String]): Unit = {
    val sc = UtilsClass.createStreaingContext("ForeachRDD")
    
    val dStream = sc.receiverStream(new TestReceiver("localhsot",9999))
    
      dStream.foreachRDD{rdd =>
        rdd.foreach(record=>record.matches("what"))
      
    }
    dStream.foreachRDD{rdd=>
      rdd.foreachPartition {record =>
        record.contains("where")
        
      }
      
    }
    
  }
}