package com.scala.spark.MLlib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

object BlockMatrix {
  
  /*
   *   A BlockMatrix is a distributed matrix backed by an RDD of MatrixBlocks,
   *   where a MatrixBlock is a tuple of ((Int, Int), Matrix), where the (Int, Int) is the index of the block,
   *   and Matrix is the sub-matrix at the given index with size rowsPerBlock x colsPerBlock. 
   *   BlockMatrix supports methods such as add and multiply with another BlockMatrix. 
   *   BlockMatrix also has a helper function validate which can be used to check whether the BlockMatrix is set up properly.
   *  
   *   A BlockMatrix can be most easily created from an IndexedRowMatrix or CoordinateMatrix by calling toBlockMatrix.
   *   toBlockMatrix creates blocks of size 1024 x 1024 by default. 
   *   Users may change the block size by supplying the values through toBlockMatrix(rowsPerBlock, colsPerBlock).
   * */
  
  def main(args:Array[String]){
  
    // Creating Block matrix from Coordinatematrix
    
    val conf = new SparkConf().setMaster("local").setAppName("CoordinationMatrix")
    val context = new SparkContext(conf)
    
    
    val m1 = new MatrixEntry(1L,2L,1.0)
    val m2 = new MatrixEntry(2L,3L,2.0)
    val m3 = new MatrixEntry(3L,4L,3.0)
    val m4 = new MatrixEntry(4L,5L,4.0)
    
    val rows = context.parallelize(Seq(m1,m2,m3,m4))
    
    val blockMatrix = new CoordinateMatrix(rows).toBlockMatrix()
    
     println("Matrix size::"+"("+blockMatrix.numRows()+","+blockMatrix.numCols()+")")
    
    // creating blockmatrix from IndexedRowMatrix
     
      val v1 = Vectors.dense(Array(1.0,2.0,3.0,4.0))
    val iRow1:IndexedRow = new IndexedRow(1L,v1)
    
    val v2 = Vectors.dense(Array(3.0,5.0,4.0,2.0))
    val iRow2:IndexedRow = new IndexedRow(2L,v2)
    
    val v3 = Vectors.dense(Array(7.0,6.0,2.0,9.0))
    val iRow3:IndexedRow = new IndexedRow(3L,v3)
    
    val rows1 = context.parallelize(Seq(iRow1,iRow2,iRow3))
   
    // creating row indexed matrix using RDD[IndexedRow]
    val blockMat = new IndexedRowMatrix(rows1).toBlockMatrix()
    println("Matrix size::"+"("+blockMat.numRows()+","+blockMat.numCols()+")")
    
    // adding two block matrices
    
    val finalmat = blockMat.add(blockMatrix)
    println("finalmat size::"+"("+finalmat.numRows()+","+finalmat.numCols()+")")
    
    //Multiplying two block matrices
    
    val multiBlockMat = blockMat.multiply(blockMatrix)
    println("finalmat size::"+"("+multiBlockMat.numRows()+","+multiBlockMat.numCols()+")")
    
    
  }
}