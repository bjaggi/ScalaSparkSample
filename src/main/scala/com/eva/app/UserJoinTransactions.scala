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

// this class will create a report of all user transactions
class UserTranxtnReport(sc: SparkContext) {
  def run(t: String, u: String) : RDD[(Int, ((Int, String), (String, String, String)))]  = {
        val transactions = sc.textFile(t)
	val newTransactionsPair = transactions.map{t =>                
	    val p = t.split("\t")
	    (p(0).toInt, (p(3).toInt,p(4).toString) )
	}
	
	val users = sc.textFile(u)
	val newUsersPair = users.map{t =>                
	    val p = t.split("\t")
	    (p(0).toInt, ( p(1).toString, p(2).toString,p(3).toString ))
	}
	
	val result = processData(newTransactionsPair, newUsersPair)
	return sc.parallelize(result.collect())
  } 
  
  def processData (t: RDD[(Int, (Int, String ) )], u: RDD[(Int, (String,String, String)) ]) : RDD[(Int, ((Int, String), (String, String, String)))]  = {
	var jn = t.join(u)
	//println(jn)
	println( jn.collect() )
	return jn
  }
}


object UserJoinTransactions {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Softwares\\hadoop-common-2.2.0-bin-master");
    
     

        
        val transactionsIn =  "C:\\workspace\\transactions.txt"; //args(1)
        val usersIn = "C:\\workspace\\users.txt"; // args(0)
        val conf = new SparkConf().setAppName("SparkJoins").setMaster("local").set("spark.ui.port","18080");
        val context = new SparkContext(conf)
        //val job = new UserJoinTransactions(context)
        val job = new UserTranxtnReport(context)
        
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val outputDir = "\\workspace\\users_transactions"; //args(2)
  /*      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://192.168.56.1:18080"), hadoopConf)
    try { 
      hdfs.delete(new org.apache.hadoop.fs.Path(outputDir), true) 
      } catch { 
        case e: Throwable => {println(e) }        
        }*/
      
      
        val results = job.run(transactionsIn, usersIn)
       results.saveAsTextFile(outputDir)
        context.stop()
  }
}