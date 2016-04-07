package com.scala.spark.MLlib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

object CoordinationMatrix {
  
  /*A CoordinateMatrix is a distributed matrix backed by an RDD of its entries. 
    Each entry is a tuple of (i: Long, j: Long, value: Double), where i is the row index, j is the column index,
    and value is the entry value. A CoordinateMatrix should be used only 
    when both dimensions of the matrix are huge and the matrix is very sparse.

   * A CoordinateMatrix can be created from an RDD[MatrixEntry] instance,
     where MatrixEntry is a wrapper over (Long, Long, Double).
     A CoordinateMatrix can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix. 
     Other computations for CoordinateMatrix are not currently supported
   */
  
 
  def main(args:Array[String]){
    
    val conf = new SparkConf().setMaster("local").setAppName("CoordinationMatrix")
    val context = new SparkContext(conf)
    
    
    val m1 = new MatrixEntry(1L,2L,1.0)
    val m2 = new MatrixEntry(2L,3L,2.0)
    val m3 = new MatrixEntry(3L,4L,3.0)
    val m4 = new MatrixEntry(4L,5L,4.0)
    
    val rows = context.parallelize(Seq(m1,m2,m3,m4))
    
    val coMatrix = new CoordinateMatrix(rows)
    
     println("Matrix size::"+"("+coMatrix.numRows()+","+coMatrix.numCols()+")")
    
  }
}