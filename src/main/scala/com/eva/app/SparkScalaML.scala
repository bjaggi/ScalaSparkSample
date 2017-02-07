package com.eva.app

/**
 * This Machine learning algorithms will make prediction or decision based on training data :
 * -Classification : identify which of several categories an item belongs to.
 * -Example : classify if a loan will default
 * -Regression : predict the value of a continuous variable.
 * -Example : predict the future balance of a line of credit
 * -Clustering : group objects into clusters of high similarity.
 * -data exploration
 * -anomaly detection
 *
 * Two predictive models :
 * -Classification (decision tree) :
 * -How likely is the loan to result in default? (Probability)
 * -Regression (Linear regression) :
 * -What is the interest rate to be offered? (%)
 */

import org.apache.spark.ml.classification.DecisionTreeClassifier
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.udf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object SparkScalaML {

  def main(args: Array[String]) {


  }
  
 /* def buildClassificationModel() {
    //building classification model (scala)
    val df = sqlContext.table("brelib2.loan")
    val df2 = df.na.fill(0)
    val label = "bad_loan"
    var features = Array("annual_inc", "delinq_2yrs", "dti", "emp_length", "loan_amnt", "longest_credit_length", "revol_util", "total_acc")
    val df_vectorized = new VectorAssembler().setInputCols(features.toArray).setOutputCol("features").transform(df2)
    val stringIndexer = new StringIndexer().setInputCol(label).setOutputCol("label").fit(df_vectorized)
    val df_label_transformed = stringIndexer.transform(df_vectorized)
    val df_label_transformed2 = df_label_transformed.select("label", "features")
    val Array(training, validation) = df_label_transformed2.randomSplit(Array(0.6, 0.4))
    training.cache()
    val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
    val model = dt.fit(training)
    sc.parallelize(Seq(model), 1).saveAsObjectFile("hdfs:///user/brelib2/DecisionTreeClassifier.model")
  }
  
  def performanceClassificationModel() {
    val predictions = model.transform(training)
val getPOne = udf((v: org.apache.spark.mllib.linalg.Vector) => v(1))
val predictionAndLabels = predictions.select(getPOne($"probability"),$"label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val metrics = new BinaryClassificationMetrics(predictionAndLabels)
val auROC = metrics.areaUnderROC
auROC: Double = 0.6348128103649956
val predictions = model.transform(validation)
val getPOne = udf((v: org.apache.spark.mllib.linalg.Vector) => v(1))
val predictionAndLabels = predictions.select(getPOne($"probability"),$"label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val metrics = new BinaryClassificationMetrics(predictionAndLabels)
val auROC = metrics.areaUnderROC
auROC: Double = 0.6254486566670212
  }
  
    def buildingRegressionModel() {
      val df = sqlContext.table("brelib2.loan")
val df2 = df.na.fill(0)
val label = "int_rate"
var features = Array("annual_inc", "delinq_2yrs", "dti", "emp_length", "loan_amnt", "longest_credit_length", "revol_util", "total_acc")
val df_vectorized = new VectorAssembler().setInputCols(features.toArray).setOutputCol("features").transform(df2)
val df_label_transformed2 = df_vectorized.select(label,"features")
val Array(training,validation) = df_label_transformed2.randomSplit(Array(0.6, 0.4))
Training.cache()
val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol(label).setFeaturesCol("features")
val lrModel = lr.fit(training)
sc.parallelize(Seq(lrModel), 1).saveAsObjectFile("hdfs:///user/brelib2/linReg.model")
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    }

    def performanceRegressionModel(){
      val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"r2: ${trainingSummary.r2}")
r2: 0.19335639319149578
val Score = lrModel.transform(validation)
val valuesAndPreds = Score.select($"prediction",$"int_rate").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
val metrics = new RegressionMetrics(valuesAndPreds)
val R2 = metrics.r2
R2: Double = 0.19320921588490636
    }
      */
}