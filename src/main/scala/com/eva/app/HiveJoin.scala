package com.eva.app

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext

object HiveJoin {
  
  def main(args: Array[String]): Unit = {
    
    println("=========== > Starting Scala Program")
    
    val conf = new SparkConf().setAppName("hivejoin").setMaster("local");
    val sc = new SparkContext(conf)
    val  sqlContext = new HiveContext(sc);
        
    val bloanLoan = sqlContext.sql("select * from londb.blonloan");  
     val bloanFact = sqlContext.sql("select * from londb.blonfact");   
     
    val joinedDF = bloanLoan.join(bloanFact, 
         bloanLoan("branch_num")=== bloanFact("branch_number") &&
         bloanLoan("account_num")=== bloanFact("account_number") &&
         bloanLoan("lon_num")=== bloanFact("loan_number") ,
         "inner"
         )

         
         //
         
    val results = joinedDF.take(10)
    for(result<-results)
      println(result)
    
  }
   
   
}