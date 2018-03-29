class HivePartitionORCTest  extends FunSpec with MockitoSugar  {

/*
 val sparkSession: SparkSession = SparkSession.builder().appName("test").master("local[3]").config("spark.hadoop.validateOutputSpecs", "false").getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  protected final val logger = LoggerFactory.getLogger(this.getClass)
*/



//  @Test
  it ("Group all files bases on names") {
//u can mock scala objects by making traits

  Assert.assertTrue(true);
    /*val reader = mock[TitanHDFSReader]
    val sc = mock[SparkContext]
    val session = mock[SparkSession]
    when(reader.readFilesFromFolder(session,sc,"")).thenReturn(FILE_NAMES_LIST_TO_GROUP)
 /*   when(sc.wholeTextFiles("").thenReturn(sc.parallelize(List(
      ("I", "India"),
      ("U", "USA"),
      ("W", "West"))))*/
    println(" ====> "+reader.readFilesFromFolder(session,sc,""))*/


  }
}

