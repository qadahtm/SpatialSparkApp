package queries

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{ Level, Logger }
object CountTest extends Logging {

  def main(args: Array[String]) = {

   val sparkConf = new SparkConf().setAppName("YarnSpark")
    val sc = new SparkContext(sparkConf)
val aw = sc.textFile("hdfs://ip-172-30-3-197.ec2.internal:8020/user/tiger/areawater.csv.bz2",5)
val caw = aw.persist(StorageLevel.MEMORY_ONLY)
val c1 = caw.count

    println("YarnSpark: First Count = " + c1)
val c2 = caw.count

    println("YarnSpark: Second Count = " + c2)

  }
}
