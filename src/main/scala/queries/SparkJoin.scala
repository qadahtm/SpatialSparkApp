package queries

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{ Level, Logger }

object SparkJoin extends Logging {

  def main(args: Array[String]) = {

   val sparkConf = new SparkConf().setAppName("SparkReduce")
    val sc = new SparkContext(sparkConf)
        
    val maxCount = args(0).toInt
    val numPartitions = args(1).toInt
    val numSlice = args(2).toInt
    
    val a = Array.tabulate(maxCount)(i => (1,i))
    
    val pa = sc.parallelize(a, numSlice)
    
    val out = pa.reduceByKey(_ + _, numPartitions).collect

    println("SparkJoin: out size = " + out.size)
    println("SparkJoin: Sum = " + out(0)) 

  }
}
