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
import org.apache.log4j.{Level, Logger}




object Kafka extends App with Logging {
	Console.println("Getting Started with Spark")
	
	setStreamingLogLevels()
	
//	val logFile = "/Users/qadahtm/Research/MM/Data/BerlinMOD_0_005_CSV_new/trips.csv"
	val conf = new SparkConf().setAppName("BerlinMOD")
    val sc = new SparkContext(conf)
	val ssc = new StreamingContext(sc, Seconds(1))
	ssc.checkpoint("checkpoint")
	
	val lines = KafkaUtils.createStream(ssc, "localhost:2181", "singleApp-group", Map("test" -> 1)).map(_._2)
	
	val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(4), Seconds(2), 2)
      
//	wordCounts.print()
	
    wordCounts.foreachRDD(rdd => {
      
      val props = new java.util.Properties()
			props.put("metadata.broker.list", "localhost:9092")
			props.put("serializer.class", "kafka.serializer.StringEncoder")
			props.put("request.required.acks", "1")
		 
			val pconfig = new ProducerConfig(props)
			val producer = new Producer[String, String](pconfig)
    
		rdd.collect.foreach{
    	  	record =>{
              val data = new KeyedMessage[String, String]("out", record._1, record._2.toString)
    		  producer.send(data)    		  
            }
          }
      
      producer.close
//      rdd.foreachPartition{
//        part => {
//          val props = new java.util.Properties()
//			props.put("metadata.broker.list", "localhost:9092")
//			props.put("serializer.class", "kafka.serializer.StringEncoder")
//			props.put("request.required.acks", "1")
//		 
//			val pconfig = new ProducerConfig(props)
//			val producer = new Producer[String, String](pconfig)
//          //println("*********  here is a record: "+record)
//			
//          part.foreach{
//            record =>{
//              val data = new KeyedMessage[String, String]("out", record._1, record._2.toString)
//    		  producer.send(data)    		  
//            }
//          }
//          producer.close
//        }
//      }
    })
    

    ssc.start()
    ssc.awaitTermination()
    
    def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }
	
}
