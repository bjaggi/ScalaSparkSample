package com.eva.app

  
 import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.collection.Map
/**
 * this file reads two text files and then joins data in the files.
 */

 class UserJoinTransactions(sc: SparkContext) {
  def run(t: String, u: String) : RDD[(String, String)] = {
        val transactions = sc.textFile(t)
	val newTransactionsPair = transactions.map{t =>                
	    val p = t.split("\t")
	    (p(2).toInt, p(1).toInt)
	}
	
	val users = sc.textFile(u)
	val newUsersPair = users.map{t =>                
	    val p = t.split("\t")
	    (p(0).toInt, p(3))
	}
	
	val result = processData(newTransactionsPair, newUsersPair)
	return sc.parallelize(result.toSeq).map(t => (t._1.toString, t._2.toString))
  } 
  
  def processData (t: RDD[(Int, Int)], u: RDD[(Int, String)]) : Map[Int,Long] = {
	var jn = t.leftOuterJoin(u).values.distinct
	return jn.countByKey
  }
}

object UserJoinTransactions {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master");
    
        val transactionsIn =  "C:\\workspace\\transactions.txt"; //args(1)
        val usersIn = "C:\\workspace\\users.txt"; // args(0)
        val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
        val context = new SparkContext(conf)
        val job = new UserJoinTransactions(context)
        val results = job.run(transactionsIn, usersIn)
        val output = "C:\\workspace\\users_transactions.txt"; //args(2)
        results.saveAsTextFile(output)
        context.stop()
  }
}