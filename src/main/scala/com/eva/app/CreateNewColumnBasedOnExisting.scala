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
    val UUIDGenUdf = udf(getConcatdColumns _)

    logger.info(s" Schema : $df.printSchema()")
    logger.error("testing errors ")

    //println(s"====> " , df.printSchema())
    df = df.withColumn("newColumn", UUIDGenUdf($"TRANS_DATE", $"TRANS_TIME", $"PROCESSING_SEQ_NUM"))
    df.write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv").option("header", "true")
      .save("output_files/op.csv")


  }

  
  def getConcatdColumns(bbcDate: String, bbcTime: String, bbcSeqNum: String): String = {
    EpochTime.timeinms(bbcDate) + bbcTime.substring(0, 6) + bbcSeqNum
  }
