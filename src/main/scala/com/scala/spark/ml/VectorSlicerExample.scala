package com.scala.spark.ml

import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.attribute.{AttributeGroup,Attribute,NumericAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.feature.VectorSlicer

object VectorSlicerExample {
  def main(args: Array[String]): Unit = {
    val data = Array(Row(Vectors.dense(-2.0,2.3,0.0)))
    val defaultAttr = NumericAttribute.defaultAttr
    val attr = Array("f1","f2","f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userfeatures",defaultAttr.asInstanceOf[Array[Attribute]])
    val sc = UtilsClass.getSparkContext("VectorSlicerExample")
    
    val dataRDD = sc.parallelize(data)
    val dataSet = new SQLContext(sc).createDataFrame(dataRDD,StructType(Array(attrGroup.toStructField())))
    
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))
    val vSlicer = new VectorSlicer().
                  setInputCol("userfeatures").
                  setOutputCol("result").
                  setIndices(Array(1)).
                  setNames(Array("f3")).
                  transform(dataSet).
                  select("userFeatures","result").first()
    
    
    
  }
}