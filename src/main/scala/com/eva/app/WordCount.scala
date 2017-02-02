package com.eva.app

import org.apache.spark._
import org.apache.spark.rdd.RDD

class WordCountProcessor {
  //methods have been moved to a class to unit test it
  def getWordsByLine(wordsByLine: RDD[String]): RDD[(String)] = {
    wordsByLine.flatMap(line => line.split(" "))

    /*val counts = wordsByLine.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)*/

  }
}

object WordCount {

  System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master\\");
  val conf = new SparkConf().setAppName("wordCount").setMaster("local")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)

  /**
   * Main method is for counting word using spark submit
   */
  def main(args: Array[String]): Unit = {

    //val textFile = sc.textFile("hdfs:///user/cloudera/words.txt")
    val textFile = sc.textFile("C:\\workspace\\spark_test.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    //counts.saveAsTextFile("hdfs:///user/cloudera/spark_output/wordscount")
    counts.saveAsTextFile("C:\\workspace\\spark_output2.txt")
    println("====> ")
    counts.collect().foreach(println)
  }

}