package sql

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream


object StreamSQLExample {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

//    if (args.length < 2) {
//      System.err.println("Usage: StreamSQLExample  <hostname> <port>")
//      System.exit(1)
//    }

    val sparkConf = new SparkConf().setAppName("StreamSQLExample")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val streamSqlContext = new StreamSQLContext(ssc)

    // Create an DStream of Person objects and register it as a stream.
    val people: DStream[Person] = ssc.socketTextStream("ibnalhaytham.cs.purdue.edu", 1234).map(_.split(",")).map(p => Person(p(0), p(1).toInt))
    val x  = streamSqlContext.createSchemaDStream(people)
    x.registerAsStream("people")

    val teenagers = streamSqlContext.streamSql("SELECT name FROM people WHERE age >= 30 AND age <= 35")

    // The results of SQL queries are themselves DStreams and support all the normal operations
    teenagers.map(t => "Name: " + t(0)).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}

