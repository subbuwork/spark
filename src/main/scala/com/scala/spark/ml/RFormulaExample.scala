package com.scala.spark.ml

import org.apache.spark.ml.feature.RFormula

object RFormulaExample {
  
  /*RFormula selects features specified by R model Formula, produces vector column of features and double column of labels
   * like when formula used in R for linear regression, string input column will encoded using one hot encoder and numeric values 
   * will be casted to double
   *  
   * formula clicked ~ hour+country means, predicting clicked based on hour and country
   * */
  
  def main(args: Array[String]): Unit = {
   val sqlContext = UtilsClass.createSQLContext("RFormulaExample")
   val dataFrame = sqlContext.createDataFrame(Seq(
       (7,"usa",18,1.0),
       (2,"nz",12,0.0),
       (9,"ca",15,0.0))).toDF("id","country","hour","clicked")
       
       val formula = new RFormula()
                     .setFormula("clicked~hour+country")
                     .setFeaturesCol("features")
                     .setLabelCol("label")
                     .fit(dataFrame)
                     .transform(dataFrame)
                     .select("label","features").show()
    }
}