package com.eva.app

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, TimestampType}
import org.apache.log4j._
import org.apache.spark.sql.SaveMode

/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object TTMAnalytics {
  //Not required on Mac, Windows Only
  System.setProperty("hadoop.home.dir", "C:\\Softwares\\WinUtils\\");

  /**
    * Main method is for counting word using spark submit
    */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .master("local")
      .appName("TTMAnalytics-APP")
      .config("spark.hadoop.hive.cli.print.header", "true")
      .config("header", "true")
      //.config("delimiter","")
      .getOrCreate()


    // Convert our csv file to a DataSet, using our ClickStream case class to infer the schema.
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
      .withColumn("timeStampTS", unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
      .withColumn("timeStamp_ms", unix_timestamp($"timeStamp", "yyyy-MM-dd'T'HH:mm:ss").cast(DataTypes.LongType))

    extracted2.cache()
    val extracted3 = extracted2
      .groupBy("clientIpPort")
      .agg(max("timeStamp_ms") - min("timeStamp_ms"))
      .toDF("clientIpPort", "_2").withColumnRenamed("_2", "Session_Time")
      .filter($"Session_Time" > 0)
      .orderBy($"Session_Time".desc)
    //.show(50)

    extracted3.show(10);
    extracted3.cache()

    println("\n\n\n")
    println(" Average Session Time ")
    extracted3.agg(
      avg($"Session_Time").as("Avg_Session_Time")
    ).show()
    extracted3.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("OutPutProcessedData/goal_2_average_session_time.txt")


    //Goal3 :Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    println("\n\n\n")
    println(" Determine unique URL visits per session ")
    val extractedClickStreamGroupedByIPURL = extractedClickStream.groupBy("clientIpPort")
      //extractedClickStreamGroupedByIPURL.agg(collect_set("url"))
      .agg(collect_set("url")).show(20, false)



    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    extractedClickStream.createOrReplaceTempView("click_stream")
    val sortedUniqueUrl = spark.sql("SELECT clientIpPort , count(distinct url) as distinct_url_count, collect_set(url) as UNIQUE_URLS   FROM click_stream " +
      "                                        GROUP BY clientIpPort Order By distinct_url_count desc ")

    // Print on the Console
    //val results = sortedUniqueUrl.collect()
    //results.foreach(println)

    //Write to File
    /*    sortedUniqueUrl.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("OutPutProcessedData/goal_3_unique_visit_per_session.txt")*/


    //Goal4 : Find the most engaged users, ie the IPs with the longest session times
    println("\n\n\n")
    println(" Most engaged users, ie the IPs with the longest session times")
    extracted3.show(20)
    extracted3.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv("OutPutProcessedData/goal_4_most_active_users.txt")

    spark.stop()

  }

  /**
    *
    * @param line
    * @return
    */
  def mapper(line:String): ClickStream = {
    val parts = line.split(" ")

    val clickStream:ClickStream = new ClickStream(parts(0), parts(2), parts(5), parts(12))
    return clickStream
  }

  /**
    *
    * @param timeStamp
    * @param clientIpPort
    * @param backendProcessingTime
    * @param url
    */
  case class ClickStream(timeStamp:String, clientIpPort : String, backendProcessingTime : String , url : String )

}
