package com.scala.spark.MLlib

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

object IndexedRowMatrix {
  
  /*
   * An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices.
   *  It is backed by an RDD of indexed rows, so that each row is represented by its index (long-typed) and a local vector.
   * 
   * An IndexedRowMatrix can be created from an RDD[IndexedRow] instance, where IndexedRow is a wrapper over (Long, Vector). 
   * An IndexedRowMatrix can be converted to a RowMatrix by dropping its row indices. 
   * */
  
  
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("IndexedRowMatrix")
    val context = new SparkContext(conf)
    
    
    val v1 = Vectors.dense(Array(1.0,2.0,3.0,4.0))
    val iRow1:IndexedRow = new IndexedRow(1L,v1)
    
    val v2 = Vectors.dense(Array(3.0,5.0,4.0,2.0))
    val iRow2:IndexedRow = new IndexedRow(2L,v2)
    
    val v3 = Vectors.dense(Array(7.0,6.0,2.0,9.0))
    val iRow3:IndexedRow = new IndexedRow(3L,v3)
    
    val rows = context.parallelize(Seq(iRow1,iRow2,iRow3))
   
    // creating row indexed matrix using RDD[IndexedRow]
    val iRowMatrix = new IndexedRowMatrix(rows)
    
    val cols = iRowMatrix.numCols()
    val rows1 = iRowMatrix.numRows()
    
    println("Matrix size::"+"("+rows1+","+cols+")")
  }
}