package com.scala.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Matrix,Matrices}

object LocalMatrix {
  
  /*
   * A local matrix has integer-typed row and column indices and double-typed values, 
   * stored on a single machine. MLlib supports dense matrices, whose entry values are stored in a single double array in column-major order,
   *  and sparse matrices, whose non-zero entry values are stored in the Compressed Sparse Column (CSC) format in column-major order. For example,
   *   the following dense matrix
         (1.03.05.02.04.06.0)
         (1.02.03.04.05.06.0)
      is stored in a one-dimensional array [1.0, 3.0, 5.0, 2.0, 4.0, 6.0] with the matrix size (3, 2)
   
   * 
   * The base class of local matrices is Matrix, and we provide two implementations: DenseMatrix,
   *  and SparseMatrix. We recommend using the factory methods implemented in Matrices to create local matrices. Remember,
   *   local matrices in MLlib are stored in column-major order.

   * */
  
  
  
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("LocalMatrix")
    val context = new SparkContext(conf)
    
    // Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
    val denseMatrix:Matrix = Matrices.dense(3,2, Array(1.0,3.0,5.0,2.0,4.0,6.0))
    val cols=denseMatrix.numCols
    val rows=denseMatrix.numRows
    
    // Create a sparse matrix ((9.0, 0.0), (0.0, 8.0), (0.0, 6.0))
    val sparseMatrix:Matrix =Matrices.sparse(3, 2, Array(0,1,3),Array(0,1,2),Array(6,8,9))
    val col = sparseMatrix.numCols
    val row = sparseMatrix.numRows
    
    println("dense::"+"("+cols+","+rows+")"+":: sparse::"+"("+col+","+row+")")
  
  
  }
}