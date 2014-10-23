package queries

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{ Level, Logger }
import utils.Point2D
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import spray.json.JsObject
import spray.json.JsNumber
import spray.json.JsArray
import spray.json.JsString
import utils.RangeQuery
import utils.Tweet
import utils.SObject
import utils.Helper
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkEnv
import scala.concurrent.Await
import akka.actor.Props
import utils.SparkSampleActorReceiver
import utils.Forward
import org.apache.spark.streaming.dstream.DStream
import utils.OutputHelper
import utils._

object MovingRange extends Logging {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val appconf = utils.Helper.getConfig

    val delimit = ","
    val delimit2 = ":"

    // actor based
    val sourceActorSystemName = appconf.getString("twitterApi.feeder.actorSystemName")
    val sourceHost = appconf.getString("twitterApi.feeder.hostname")
    val sourcePort = appconf.getString("twitterApi.feeder.port")
    val sourceActorName = appconf.getString("twitterApi.feeder.name")

    val webActorSystemName = appconf.getString("webserver.actorSystem.name")
    val webHost = appconf.getString("webserver.hostname")
    val webPort = appconf.getString("webserver.actorSystem.port")
    val queriesActorName = appconf.getString("webserver.queryPubsubActorName")

    val conf = new SparkConf().setAppName("Twitter")

    val sc = new SparkContext(conf)
    val sparkasys = SparkEnv.get.actorSystem
    val ssc = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("checkpoint")

    val source = ssc.actorStream[String](Props(new SparkSampleActorReceiver[String]("akka.tcp://" + sourceActorSystemName + "@" + sourceHost + ":" + sourcePort + "/user/" + sourceActorName)), "TweetReceiver")
    val queriesSource = ssc.actorStream[String](Props(new SparkSampleActorReceiver[String]("akka.tcp://" + webActorSystemName + "@" + webHost + ":" + webPort + "/user/" + queriesActorName)), "QueryReceiver")

    //  source.print()
    //  queriesSource.print()

    // Transofrtmation
    val queryList = queriesSource.map(line => {

      val arr = line.split(delimit)
      val qid = arr(0)

      val regionPoints = (arr.slice(1, arr.length)).map(pstr => {
        val parr = pstr.split(delimit2)

        log.info(pstr)

        Point2D(parr(0).toDouble, parr(1).toDouble)
      })

      (1, Seq(RangeQuery(qid, regionPoints, DateTime.now().getMillis())))
    }).reduce((v1, v2) => {
      if (v1._2(0).equals(v2._2(0))) {
        if (v1._2(0).ts >= v2._2(0).ts) v1 else v2
      } else {
        (1, v1._2 ++ v2._2)
      }
    })

    val queries = queryList.updateStateByKey[Seq[RangeQuery]](updateQueriesState _)

    queries.print
    
    // Processing	  	  

    // Spatial Twitter Stream processing

    // tweetid,createdAt,lat,lng,text

    val tweets = source.map(line => {
      val arr = line.split(",")
      val tweetId = arr(0).toLong
      val createdAt = arr(1)
      val lat = arr(2).toDouble
      val lng = arr(3).toDouble
      val text = arr(4)

      (1, Tweet(createdAt, text, SObject(tweetId, lng, lat)))
    })

    val output = tweets.join(queries).flatMap {
      // (K,(T,Q))
      case (_, (t, qs)) => {
        qs.flatMap(q => {
          if (q.isInsideRange(t.spatialPrams)) {
            Some((q, t))
          } else None
        })
      }
    }

    output.print()
    val outputStreamer = new OutputHelper[(RangeQuery, Tweet)]()
    outputStreamer.writeOutput(output)(out => {
      // for each record in the DStream
      // take the second element in the tuple
      val secondValue = out._2 
      // return the string value
      secondValue.toString
    })
    
    // Output the source to the UI
//    source.print
//    val outputStreamer = new OutputHelper[String]()
//    outputStreamer.writeOutput(source)(x => x)

    scala.sys.addShutdownHook({
      source.getReceiver.stop("stopit")
      ssc.stop(true)
    })

    ssc.start()
    ssc.awaitTermination()

    

  }
  
  def updateQueriesState(newqueries: Seq[Seq[RangeQuery]], prevState: Option[Seq[RangeQuery]]): Option[Seq[RangeQuery]] = {
      prevState match {
        case Some(qs) => {
          if (newqueries.size == 0) {
            Some(qs)
          } else if (newqueries.size == 1) {
            val nq = newqueries(0)
            val nqs = qs.filter(!nq.contains(_))
            Some(nqs ++ nq)
          } else {
            val nnqs = newqueries.reduce((sq1, sq2) => {
              val fsq2 = sq2.filter(sq1.contains(_))
              sq1 ++ fsq2
            })
            val nqs = qs.filter(!nnqs.contains(_))
            Some(nqs ++ nnqs)
          }
        }
        case None => {
          Some((newqueries.reduce(_ ++ _)))
        }
      }
    }

}