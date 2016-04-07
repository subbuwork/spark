package com.scala.spark.ml

import scala.collection.mutable
import scala.language.reflectiveCalls
import scopt.OptionParser
import scopt.OptionParser


object LogisticRegressionExample {
  
  case class Params(
      
                   input           : String  = null,
                   testInput       : String  =  "",
                   dataFormat      : String  = "libsvm",
                   regParam        : Double  = 0.0,
                   elasticNetparam : Double  = 0.0,
                   maxIter         : Int     = 100,
                   fitIntercept    : Boolean = true,
                   tol             : Double  = 1E-6,
                   fracTest        : Double  = 0.2)extends AbstractParams[Params]
  
  def main(args:Array[String]){
    
    val defaultParams = Params()
    
    val parser = new OptionParser[Params]("LogisticRegressionExample"){
                 head("LogisticRegressionExample: an example Logistic Regression with Elastic-Net app.")
                 opt[Double]("reParam")
                 .text(s"regularization parameter, default: ${defaultParams.regParam}")
                 .action((x,c)=>c.copy(regParam=x))
                 
                 opt[Double]("elasticNetparam")
                 .text(s"ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. " +
                       s"For alpha = 1, it is an L1 penalty. For 0 < alpha < 1, the penalty is a combination of " +
                       s"L1 and L2, default: ${defaultParams.elasticNetparam}")
                 .action((x,c)=>c.copy(elasticNetparam = x))
                 
                 opt[Int]("maxIter")
                 .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
                 .action((x,c)=>c.copy(maxIter = x))
                 
                 opt[Boolean]("fitIntercept")
                 .text(s"whether to fit an intercept term, default: ${defaultParams.fitIntercept}")
                 .action((x,c)=>c.copy(fitIntercept = x))
                 
                 opt[Double]("tol")
                  .text(s"the convergence tolerance of iterations, Smaller value will lead " +
                        s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
                  .action((x,c)=>c.copy(tol = x))
                 
                 opt[Double]("fracTest")
                    .text(s"fraction of data to hold out for testing.  If given option testInput, " +s"this option is ignored. default: ${defaultParams.fracTest}")
                    .action((x,c)=>c.copy(fracTest = x))
                 
                 opt[String]("testInput")
                  .text(s"input path to test dataset.  If given, option fracTest is ignored." +s" default: ${defaultParams.testInput}")
                  .action((x,c)=>c.copy(testInput = x))
                  
                opt[String]("dataFormat")
                  .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
                  .action((x,c)=>c.copy(dataFormat = x))
                  
                opt[String]("<input>") 
                 .text("input path to labeled examples").required()
                 .action((x,c)=>c.copy(input = x))
    
                  checkConfig { params =>
                    if(params.fracTest < 0 || params.fracTest >= 1){
                      failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
                    }else{
                      success
                    }
                   
                 }   
        }
  
    
    parser.parse(args,defaultParams).map{params =>
      run(params)
    
  }.getOrElse{
    sys.exit(0)
  }
  
  }
  
  def run(params:Params){
    
    val context = UtilsClass.getSparkContext("LogisticRegressionExample")
    
    
    
    
    
    
    
    
    
    
    
    
  }
  
  
  
  
}