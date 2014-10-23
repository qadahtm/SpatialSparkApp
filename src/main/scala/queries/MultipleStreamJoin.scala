package queries

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{ Level, Logger }
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import utils.Point2D
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import utils.MObject
import utils.RangeQuery
import utils.SObject
import utils.Helper

object MultipleStreamJoinKafka extends App with Logging {
  
  val delimit_comma = ","
  val delimit_colon = ":"
  val outTopic = "output"


  // Spark Platform Configuration   
  val conf = new SparkConf().setAppName("BerlinMOD")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("checkpoint")

  // open stream source
  val source = KafkaUtils.createStream(ssc, "localhost:2182", "singleApp-group", Map("input" -> 1)).map(_._2)
  val queriesSource = KafkaUtils.createStream(ssc, "localhost:2182", "singleApp-group", Map("queries" -> 1))
  					  .map(_._2).window(Seconds(300)) // keep queries for the 300 seconds
  
  // Processing	  	  

  // Query Stream Processing
  					  
  val queryList = queriesSource.map(line =>{
  
	  val arr = line.split(delimit_comma)
	  val qid = arr(0)
	  
	  
	  val regionPoints = (arr.slice(1, arr.length)).map(pstr => {
	  val parr = pstr.split(delimit_colon)
	  
	    log.info(pstr)
	    
	    Point2D(parr(0).toDouble,parr(1).toDouble)
	  })	  
	  
	  (1,RangeQuery(qid,regionPoints, DateTime.now().getMillis()))})
	  
  // BerlinMod data processing
 
  // extract location updates from trips.csv using only the start of the trip point. 
  val locationUpdates = source.flatMap(line => {
    var res = Array[(Long,MObject)]()
    val arr = line.split(",")
    // CSV line used in trips.csv data
    // Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend      
    try {

      val moid = arr(0).toLong
      val tripid = arr(1)
      // Timestamp format used in BerlinMOD data
      // 2007-05-28 08:41:15.434 
      
      val datetimeFormatLong = "YYYY-MM-dd HH:mm:ss.SSS"
      val fmtLong =  DateTimeFormat.forPattern(datetimeFormatLong)
      val datetimeFormatShort = "YYYY-MM-dd"
      val fmtShort =  DateTimeFormat.forPattern(datetimeFormatShort)
      
      var tstart = DateTime.now()
      
      try {
        tstart = DateTime.parse(arr(2), fmtLong) 
      }
      catch {
        case e:java.lang.IllegalArgumentException =>{
          tstart = DateTime.parse(arr(2), fmtShort)
        }
      }
      
      val xstart = arr(4).toDouble
      val ystart = arr(5).toDouble

      // ignored for now
//      val tend = arr(3)
//      val xend = arr(6).toDouble
//      val yend = arr(7).toDouble

      val ssobj = SObject(moid, xstart, ystart)
//      val esobj = SObject(moid, xend, yend)

      res = Array((moid,MObject(tstart, ssobj)) //, (tend,esobj)
      )

    } catch {
      case e: NumberFormatException => {
        e.printStackTrace()        
      }
    }
    res
  }).reduceByKey((m1,m2) => {
    // get latest update only for each key
    if (m1.t.getMillis() >= m2.t.getMillis()) m1 else m2    
  }).map( loc => {
    (1, loc)
  })
  
  val output = locationUpdates.join(queryList).flatMap{
    case (_,(p,q)) => {
    	if (q.isInsideRange(p._2.sobj)){
    	  Some("QueryID = "+q.qid+", ObjectID = "+p._1)
    	}
    	else None
    }
  }
   
   // debug prining
//  output.print()

  // output  
  output.foreachRDD(rdd => {

    rdd.foreachPartition(partitionOfRecords => {

      val producer = Helper.createKafkaProducer()

      partitionOfRecords.map {
        record =>
          {
        	  val data = new KeyedMessage[String, String](outTopic, record)
			  producer.send(data)
          }
      }

      producer.close
    })

  })

  ssc.start()
  ssc.awaitTermination()
  
  
}


object MultipleStreamJoinNetwork extends App with Logging {
  
  val delimit_comma = ","
  val delimit_colon = ":"
  val outTopic = "output"
    
  val dataHost:String = "localhost"
  val dataPort:Int = 9910
  
  val queryHost:String = "localhost"
  val queryPort:Int = 9911


  // Spark Platform Configuration   
  val conf = new SparkConf().setAppName("BerlinMOD")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("checkpoint")

  // open stream source
   
  val source = ssc.socketTextStream(dataHost, dataPort, StorageLevel.MEMORY_AND_DISK_SER)
  val queriesSource = ssc.socketTextStream(dataHost, dataPort, StorageLevel.MEMORY_AND_DISK_SER)
  						.window(Seconds(300)) // keep queries for the 300 seconds
  
  // Processing	  	  

  // Query Stream Processing
  					  
  val queryList = queriesSource.map(line =>{
  
	  val arr = line.split(delimit_comma)
	  val qid = arr(0)
	  
	  
	  val regionPoints = (arr.slice(1, arr.length)).map(pstr => {
	  val parr = pstr.split(delimit_colon)
	  
	    log.info(pstr)
	    
	    Point2D(parr(0).toDouble,parr(1).toDouble)
	  })	  
	  
	  (1,RangeQuery(qid,regionPoints, DateTime.now().getMillis()))})
	  
  // BerlinMod data processing
 
  // extract location updates from trips.csv using only the start of the trip point. 
  val locationUpdates = source.flatMap(line => {
    var res = Array[(Long,MObject)]()
    val arr = line.split(",")
    // CSV line used in trips.csv data
    // Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend      
    try {

      val moid = arr(0).toLong
      val tripid = arr(1)
      // Timestamp format used in BerlinMOD data
      // 2007-05-28 08:41:15.434 
      
      val datetimeFormatLong = "YYYY-MM-dd HH:mm:ss.SSS"
      val fmtLong =  DateTimeFormat.forPattern(datetimeFormatLong)
      val datetimeFormatShort = "YYYY-MM-dd"
      val fmtShort =  DateTimeFormat.forPattern(datetimeFormatShort)
      
      var tstart = DateTime.now()
      
      try {
        tstart = DateTime.parse(arr(2), fmtLong) 
      }
      catch {
        case e:java.lang.IllegalArgumentException =>{
          tstart = DateTime.parse(arr(2), fmtShort)
        }
      }
      
      val xstart = arr(4).toDouble
      val ystart = arr(5).toDouble

      // ignored for now
//      val tend = arr(3)
//      val xend = arr(6).toDouble
//      val yend = arr(7).toDouble

      val ssobj = SObject(moid, xstart, ystart)
//      val esobj = SObject(moid, xend, yend)

      res = Array((moid,MObject(tstart, ssobj)) //, (tend,esobj)
      )

    } catch {
      case e: NumberFormatException => {
        e.printStackTrace()        
      }
    }
    res
  }).reduceByKey((m1,m2) => {
    // get latest update only for each key
    if (m1.t.getMillis() >= m2.t.getMillis()) m1 else m2    
  }).map( loc => {
    (1, loc)
  })
  
  val output = locationUpdates.join(queryList).flatMap{
    case (_,(p,q)) => {
    	if (q.isInsideRange(p._2.sobj)){
    	  Some("QueryID = "+q.qid+", ObjectID = "+p._1)
    	}
    	else None
    }
  }
   
   // debug prining
  output.print()

  ssc.start()
  ssc.awaitTermination()
  
  
}
