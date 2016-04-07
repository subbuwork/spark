package com.scala.spark.ml

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SQLContext,Row}
import org.apache.spark.ml.feature.{Tokenizer,HashingTF}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vector

object PipelineExample {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("PipelineExample")
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    
    // Prepare training documents from a list of (id, text, label) tuples.
    
    val training = sqlContext.createDataFrame(Seq((0L,"welcome to usa",1.0),
                                                   (1L,"i am new to scala",0.0),
                                                   (2L,"welcome to scala",1.0),
                                                   (3L,"spark f g h",1.0),
                                                   (4L,"hadoop map reduce ",1.0))).toDF("id","text","label")
                                                   
    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    
    val tokenizer = new Tokenizer()
        tokenizer.setInputCol("text")
        tokenizer.setOutputCol("word")
        
    val hashingTF = new HashingTF()
        hashingTF.setNumFeatures(100)
        hashingTF.setInputCol(tokenizer.getOutputCol)
        hashingTF.setOutputCol("features")
        
    val lr        = new LogisticRegression()
        lr.setMaxIter(10)
        lr.setRegParam(0.01)
        
    val pipeline = new Pipeline()
        pipeline.setStages(Array(tokenizer,hashingTF,lr))
        
    // Fit the pipeline to training documents.
        
    val model   = pipeline.fit(training)
    
    // now we can optionally save the fitted pipeline to disk
    model.save("/tmp/spark-logistic-regression-model")

    // we can also save this unfit pipeline to disk
    pipeline.save("/tmp/unfit-lr-model")

    // and load it back in during production
    val sameModel = Pipeline.load("/tmp/spark-logistic-regression-model")
    
    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = sqlContext.createDataFrame(Seq(
                                              (4L,"a b c"),
                                              (5L,"x y x"),
                                              (6L,"t u v"),
                                              (7L, "mapreduce spark"),
                                              (8L, "apache hadoop"))).toDF("id","text")
    // Make predictions on test documents
     model.transform(test).select("id","text","probability","prediction").collect().foreach { 
      case Row(id:Long,text:String,prob:Vector,pred:Double)=>
       println(s"($id, $text) --> prob=$prob, prediction=pred")      
      
      
      
    }                                         
  }
}