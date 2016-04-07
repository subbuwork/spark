package com.scala.spark.ml

import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.ml.clustering.LDA

object LDAExample {
  final val FEATURES_COL = "features"
  def main(args: Array[String]): Unit = {
    val sc = UtilsClass.getSparkContext("LDAExample")
    val sqlContext = new SQLContext(sc)
    
    val data = sc.textFile(UtilsClass.sourcePath()+"sample_lda_data.txt")
               .filter(_.nonEmpty).map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField(FEATURES_COL,new VectorUDT,false)))
    
    val dataset = sqlContext.createDataFrame(data,schema)
    
    // Train a LDA Model
    val lda     = new LDA()
                  .setK(10)
                  .setMaxIter(10)
                  .setFeaturesCol(FEATURES_COL)
   // Fit the model
   val model    = lda.fit(dataset)
   val transformed = model.transform(dataset)
   
   val ll       = model.logLikelihood(dataset)
   val lp       = model.logPerplexity(dataset)
   
   //describeTopics
   val topics   = model.describeTopics(3)
   
   // Shows the result
   topics.show(false)
   transformed.show(false)
  }
}