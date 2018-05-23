object CreateNewColumnBasedOnExisting {
  protected val logger = LoggerFactory.getLogger(this.getClass)


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FraudFilesProcessor").setMaster("local[*]")
    var readFromFolderPath: String = ""
    var saveToFolderPath: String = ""
    logger.info("Starting Fraud Files Processor Job ")

    // Get Spark Session
    val session = SparkSession.builder().
      appName("Fraud_Files_Processor").
      config("spark.sql.crossJoin.enabled", "true").
      master("local[*]").
      //enableHiveSupport().
      getOrCreate()
    conf.set("textinputformat.record.delimiter", "\n")
    readFromFolderPath = "input_files/auth_trans"

    //Reading from File
    var df = session.read.orc(readFromFolderPath)
    //df.first();
    import org.apache.spark.sql.functions.udf
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // Concatenating 3 colummns data/time and seq number
    val rbcUUIDGenUdf = udf(getConcatdColumns _)

    logger.info(s" Schema : $df.printSchema()")
    logger.error("testing errors ")

    //println(s"====> " , df.printSchema())
    //println(s"====> " , df.withColumn("newColumn", rbcUUIDGenUdf($"RBC_TRANS_DATE",$"RBC_TRANS_TIME",$"RBC_PROCESSING_SEQ_NUM")).first())
    df = df.withColumn("newColumn", rbcUUIDGenUdf($"RBC_TRANS_DATE", $"RBC_TRANS_TIME", $"RBC_PROCESSING_SEQ_NUM"))
    df.write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv").option("header", "true")
      .save("output_files/op.csv")


  }

  /**
    * Get Concatenated String based on other 3 fields
    *
    * @param rbcDate
    * @param rbcTime
    * @param rbcSeqNum
    * @return
    */
  def getConcatdColumns(rbcDate: String, rbcTime: String, rbcSeqNum: String): String = {
    EpochTime.timeinms(rbcDate) + rbcTime.substring(0, 6) + rbcSeqNum
  }
