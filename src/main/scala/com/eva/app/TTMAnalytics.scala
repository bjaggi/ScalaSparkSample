package com.eva.app

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{DataTypes, TimestampType}
/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

class TTMAnalytics {
  //methods have been moved to a class to unit test it
  def getWordsByLine(wordsByLine: RDD[String]): RDD[(String)] = {
    wordsByLine.flatMap(line => line.split(" "))

    /*val counts = wordsByLine.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)*/

  }
}

object TTMAnalytics {

  System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils\\");

  /**
   * Main method is for counting word using spark submit
   */
  def main(args: Array[String]): Unit = {

    // Use new SparkSession interface in Spark 2.0
    val spark =  SparkSession
      .builder
      .master("local")
      .appName("TTMAnalytics-APP")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
      //.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows


    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    //val lines = spark.sparkContext.textFile("SampleInputData/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    val lines = spark.sparkContext.textFile("SampleInputData/sample_click_stream.txt")
    val extractedClickStream = lines.map(mapper).toDS().cache()

    println("Here is our inferred schema:")
    extractedClickStream.printSchema()

    // extractedClickStream.foreach(println(_))
    // Goal1 : Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics)
    val extractedClickStreamGroupedByIP = extractedClickStream.groupBy("clientIpPort")
    println(" Number of Site Visited Per User  ")
    extractedClickStreamGroupedByIP.count().orderBy($"count".desc).show(20)
    //Goal2 :Determine the average session time
    //https://aws.amazon.com/blogs/big-data/create-real-time-clickstream-sessions-and-run-analytics-with-amazon-kinesis-data-analytics-aws-glue-and-amazon-athena/
    //extractedClickStreamGroupedByIP.avg("timeStamp").show(10)
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val extracted2 = extractedClickStream
      .withColumn("timeStampTS",  unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
                                                                     //2015-07-22T09:00:28.019143Z
      .withColumn("timeStamp_ms",  unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(DataTypes.LongType))

extracted2.cache()
    val extracted3 = extracted2
      .groupBy("clientIpPort")
      .agg(max("timeStamp_ms") - min("timeStamp_ms"))
      .toDF("clientIpPort", "_2").withColumnRenamed("_2","Session_Time")
      .filter($"Session_Time" > 0)
      .orderBy($"Session_Time".desc)
      //.show(50)

    extracted3.show(10);
extracted3.cache()

    extracted3.agg(
      avg($"Session_Time").as("Avg_Session_Time")
    ).show()

    /*extracted2.select($"clientIpPort",$"timestamp")
      .withColumn("timestamp",max($"timestamp"))
      .groupBy($"clientIpPort", $"timestamp")
      //.orderBy($"timestamp".desc).show(10)*/

    //Goal3 :Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    //Goal4 : Find the most engaged users, ie the IPs with the longest session times





  }

//timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time
   // timestamp   0
  // client:port is clients IpAddress & port  2
  // backend_processing_time is the time taken  5
  // request : url visited : 11
//case class ClickStream(timeStamp:String, elb:String, backEndIpPort:String, reqProcessingTime:String, backendProcessingTime : String )
case class ClickStream(timeStamp:String, clientIpPort : String, backendProcessingTime : String , url : String )
  def mapper(line:String): ClickStream = {
    val parts = line.split(" ")

    val clickStream:ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
    return clickStream
  }

}


/**
timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
*/
