package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object Rowmatrix {
  
  /*
   * A RowMatrix is a row-oriented distributed matrix without meaningful row indices,
     backed by an RDD of its rows, where each row is a local vector. 
     Since each row is represented by a local vector, 
     the number of columns is limited by the integer range but it should be much smaller in practice
    
   * A RowMatrix can be created from an RDD[Vector] instance. Then we can compute its column summary statistics and decompositions.
     QR decomposition is of the form A = QR where Q is an orthogonal matrix and R is an upper triangular matrix. 
     For singular value decomposition (SVD) and principal component analysis (PCA), please refer to Dimensionality reduction.
   */
  
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Rowmatrix")
    val context = new SparkContext(conf)
    
    val v1 = Vectors.dense(1.0,2.0,3.0)
    val v2 = Vectors.dense(4.0,5.0,6.0)
    val v3 = Vectors.dense(7.0,8.0,9.0)
   
    val rows:RDD[Vector] = context.parallelize(Seq(v1,v2,v3))
    
    val rowMatrix = new RowMatrix(rows)
    
    val numrow=rowMatrix.numRows()
    val numCol = rowMatrix.numCols()
    println(numrow+","+numCol)
    
  }
}