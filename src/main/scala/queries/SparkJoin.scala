package queries

import org.apache.spark._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{ Level, Logger }

object SparkJoin extends Logging {

  def main(args: Array[String]) = {

    // BerlinMod Processing
    val sparkConf = new SparkConf().setAppName("LocalWordCount")
    val sc = new SparkContext(sparkConf)
    
    val datafilepath = "/Users/qadahtm/Research/MM/Data/BerlinMOD_0_005_CSV_new/trips.csv"
    val minPartitions = 4

    val lines = sc.textFile(datafilepath, minPartitions)
    
    val startPoints = lines.flatMap(line => {
      val arr = line.split(",")
	      if (arr(0).length() == 1){
	        Some((arr(2),(arr(4),arr(5))))
	      }      
	      else None      
    })
    
    val endPoints = lines.flatMap(line => {
      val arr = line.split(",")
	      if (arr(0).length() == 1){
	        Some((arr(3),(arr(6),arr(7))))
	      }      
	      else None      
    })
    
    println("")

  }
}
