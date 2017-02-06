package test.scala.com.eva.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import scala.collection.mutable.WrappedArray


import com.eva.app.UserJoinTransactions

class UserJoinTransactionTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _
  val transactionsIn = "C:\\workspace\\transactions.txt"; //args(1)
  val usersIn = "C:\\workspace\\users.txt"; // args(0)
  val userTransaction = new UserJoinTransactions(sc)
     
        
  before {
    val conf = new SparkConf().setAppName("UserJoinTransactions").setMaster("local")
    sc = new SparkContext(conf)
    println(" =====>initializing spark context ")

  }

  
  test(" testing if the join was okay ") {
    val result = userTransaction.run(transactionsIn, usersIn)
    //wordsRDD.count() should be ("8")
    Assert.assertEquals(result.count(), 2)
    //assert transaction number 1 has transaction number 3 
    Assert.assertEquals(result.lookup("1").mkString(","), "3")
    //assert transaction number 1 has transaction number 3 
    Assert.assertEquals(result.lookup("2").mkString(","), "1")

  }


  
  after {
    sc.stop()
  }

}