package main.scala.com.eva.app

import org.apache.spark._
import org.apache.spark.rdd.RDD

object SentimentalAnalysis {
  System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master\\");
  val conf = new SparkConf().setAppName("sentimental_analysis").setMaster("local")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)

  /**
   * the plan is to analyse the sentiments of the web crawler file (based on a trend search ; example "donald trump"
   */
  def main(args: Array[String]): Unit = {
    println(" Starting to analyse public sentiments on Donalds decisions!!!")
    var positiveCount = 0L
    var negativeCount = 0L
    var neutralCount = 0L
    
    val textFile = sc.textFile("C:\\workspace\\trump_web_crawler.txt")
    val resultsRDD = textFile.flatMap(line => line.split(" "))
      .map(word => sentiAnalysis(word))

    //resultsRDD.foreach(println)
    val allResponseCount = resultsRDD.count
    //println(resultsRDD.countByValue)
    val resultSummary = resultsRDD.countByValue


    for ((k, v) <- resultSummary) {
      if (k == "Positive")
        positiveCount = v
      else if (k == "Negative")
        negativeCount = v
      else if (k == "Neutral")
        neutralCount = v
      //println("key: %s, value: %s\n", k, v)
    }
   
    val positivePercentage = ((positiveCount.toFloat / allResponseCount) * 100)
    println(" Trump has a positive response of " + positivePercentage + "%")

  }

  def sentiAnalysis(str: String): String = {
    if (str == "racist" || str == "biased" || str == "old" || str == "wall" || str == "ban" || str == "climate change")
      "Negative"
    else if (str == "jobs" || str == "economy" || str == "finance" || str == "growth" || str == "taxes")
      "Positive"
    else
      "Neutral"

  }
}
