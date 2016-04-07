package com.scala.spark.MLlib

import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.regression.LabeledPoint

object DataTypes {
  
  def main(args:Array[String]){
    
    /*
     * A local vector has integer-typed and 0-based indices and double-typed values, 
     * stored on a single machine. MLlib supports two types of local vectors: dense and sparse. 
     * A dense vector is backed by a double array representing its entry values, 
     * while a sparse vector is backed by two parallel arrays: indices and values. For example, 
     * a vector (1.0, 0.0, 3.0) can be represented in dense format as [1.0, 0.0, 3.0] or in sparse format as (3, [0, 2], [1.0, 3.0]), 
     * where 3 is the size of the vector.
     * */
    
    val dencyVecotr:Vector = Vectors.dense(1.2,0.0,6.7)
    
    // creating sparse vector
    
    val parseVector:Vector = Vectors.sparse(2,Array(0,1),Array(1.2,6.7))
    
    // creating sparse vector
    
    val sparseVector:Vector = Vectors.sparse(2, Seq((0,1.0),(1,6.7)))
    
    val max=sparseVector.toJson
    println(max)
    
    val mx = parseVector.toJson
    println(mx)
    

    /*
     * A labeled point is a local vector, either dense or sparse, 
     * 
     * associated with a label/response. In MLlib, labeled points are used in supervised learning algorithms. 
     * We use a double to store a label, so we can use labeled points in both regression and classification. 
     * For binary classification, a label should be either 0 (negative) or 1 (positive). For multiclass classification,
     *  labels should be class indices starting from zero: 0, 1, 2, ....
     * 
     * 
     * */
    
    val labelPointVector = LabeledPoint(1.0,Vectors.dense(1.0,0.0,2.0))
    
    // Create a labeled point with a negative label and a sparse feature vector.
    
    val labeledPointVector = LabeledPoint(0.0,Vectors.sparse(2, Array(0,1),Array(1.0,2.0)))
    
    
  }
  
  
  
}