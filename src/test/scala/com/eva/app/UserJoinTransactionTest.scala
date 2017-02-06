package test.scala.com.eva.app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import com.eva.app.UserJoinTransactions
import com.eva.app.UserTranxtnReport

//@Ignore
class UserJoinTransactionTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _
  val transactionsIn = "C:\\workspace\\transactions.txt"; //args(1)
  val usersIn = "C:\\workspace\\users.txt"; // args(0)
     
        
  before {
    val conf = new SparkConf().setAppName("UserJoinTransactions").setMaster("local")
    sc = new SparkContext(conf)
    println(" =====>initializing spark context ")

  }

  
  test(" testing if the join was okay ") {
    val userTransactionReport = new UserJoinTransactions(sc)

    val result = userTransactionReport.run(transactionsIn, usersIn)
    // equi join so all 3 records
    Assert.assertEquals(result.count(), 2)
    //assert key number 1 has transaction number 3 
    //Assert.assertEquals(result.lookup("2").mkString(","), "3")
    //assert key number 1 has transaction number 3 
    //Assert.assertEquals(result.lookup("2").mkString(","), "1")

  }

  test(" testing if the user x transaction report was crated ") {
    val userTransaction = new UserTranxtnReport(sc)

    val result = userTransaction.run(transactionsIn, usersIn)
    // 3 users and 3 transactions
    Assert.assertEquals(result.keys.count(),3)
    //assert transaction number 1 has transaction number 3 
    
    val arr = result.lookup(1).mkString(",")
    
    
    
    println("===============> " + arr )
   // could be better way
    println(arr.split(",")(2))
    Assert.assertEquals(arr.split(",")(2).replace("(",""), "matthew@test.com" )
   

  }
  

  
  after {
    sc.stop()
  }

}