package utils

import scala.collection.mutable.ArrayBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.io.File
import com.typesafe.config.ConfigFactory
import kafka.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import com.typesafe.config.Config
import org.apache.spark.streaming.dstream.DStream
import scala.concurrent.Await
import org.apache.spark.SparkEnv
import scala.concurrent.duration._
import spray.json.JsValue
import spray.json.JsObject
import spray.json.JsString
import spray.http.HttpData
import spray.json.JsNumber
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat
import spray.json.DeserializationException

class OutputHelper[T] {

  val appconf = Helper.getConfig
  
  val webActorSystemName = appconf.getString("webserver.actorSystem.name")
  val webHost = appconf.getString("webserver.hostname")
  val webPort = appconf.getString("webserver.actorSystem.port")
  
  val resultPubSub = appconf.getString("webserver.resultPubsubActorName")
  
  implicit val default_timeout: Int = 100
  implicit val default_url: String = s"akka.tcp://$webActorSystemName@$webHost:$webPort/user/"+resultPubSub
    

  def writeOutput(dstream: DStream[T], webActorSystemName: String, webHost: String, webPort: String, actorName: String, timeout: Int)(recordToString: T => String): Unit = {
    val url = s"akka.tcp://$webActorSystemName@$webHost:$webPort/user/" + actorName
    writeOutput(dstream, url, timeout)(recordToString)
  }

  def writeOutput(dstream: DStream[T])(recordToString: T => String): Unit = {
    writeOutput(dstream, default_url, default_timeout)(recordToString)
  }

  def writeStringOutput(record: String, url: String, timeout: Int): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = timeout seconds
    val printer = Await.result(sparkasys.actorSelection(url).resolveOne(_timeout), _timeout)
    printer ! record
  }

  def writeStringOutput(record: String): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val printer = Await.result(sparkasys.actorSelection(default_url).resolveOne(default_timeout seconds), default_timeout seconds)
    printer ! record
  }

  def writeStringArrayOutput(recordi: Iterator[String], url: String, timeout: Int): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = timeout seconds
    val printer = Await.result(sparkasys.actorSelection(url).resolveOne(_timeout), _timeout)
    recordi.foreach(record => {
      printer ! record
    })
  }

  def writeStringArrayOutput(recordi: Iterator[String]): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = default_timeout seconds
    val printer = Await.result(sparkasys.actorSelection(default_url).resolveOne(_timeout), _timeout)
    recordi.foreach(record => {
      printer ! record
    })
  }

  def writeStringPairOutput(recordPair: (String, String), url: String, timeout: Int): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = timeout seconds
    val printer = Await.result(sparkasys.actorSelection(url).resolveOne(_timeout), _timeout)
    printer ! recordPair
  }

  def writeStringPairOutput(recordPair: (String, String)): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val printer = Await.result(sparkasys.actorSelection(default_url).resolveOne(default_timeout seconds), default_timeout seconds)

    printer ! recordPair
  }

  def writeStringPairArrayOutput(recordPairs: Iterator[(String, String)], url: String, timeout: Int): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = timeout seconds
    val printer = Await.result(sparkasys.actorSelection(url).resolveOne(_timeout), _timeout)
    recordPairs.foreach(recordPair => {
      printer ! recordPair
    })
  }

  def writeStringPairArrayOutput(recordPairs: Iterator[(String, String)]): Unit = {
    val sparkasys = SparkEnv.get.actorSystem
    val _timeout = default_timeout seconds
    val printer = Await.result(sparkasys.actorSelection(default_url).resolveOne(_timeout), _timeout)
    recordPairs.foreach(recordPair => {
      printer ! recordPair
    })
  }

  def writeOutput(dstream: DStream[T], url: String, timeout: Int)(recordToString: T => String): Unit = {
    dstream.foreachRDD(rdd => {
      // by partition
      rdd.foreachPartition(partition => {

        val sparkasys = SparkEnv.get.actorSystem
        val _timeout = timeout seconds
        val printer = Await.result(sparkasys.actorSelection(url).resolveOne(_timeout), _timeout)

        partition.foreach(record => {
          printer ! Forward(recordToString(record))
        })

      })
    })
  }

}
object Helper {

  val datetimeFormatLong = "YYYY-MM-dd HH:mm:ss.SSS"
  val fmtLong = DateTimeFormat.forPattern(datetimeFormatLong)

  val dfs1 = "YYYY-MM-dd HH:mm:ss"
  val dfmtm1 = DateTimeFormat.forPattern(dfs1)

  val dfs2 = "YYYY-MM-dd HH:mm"
  val dfmtm2 = DateTimeFormat.forPattern(dfs2)

  val datetimeFormatShort = "YYYY-MM-dd"
  val fmtShort = DateTimeFormat.forPattern(datetimeFormatShort)

  val formatMap = Map(
    (datetimeFormatLong.length() -> fmtLong), (datetimeFormatShort.length() -> fmtShort), (dfs1.length() -> dfmtm1), (dfs2.length() -> dfmtm2))

  def parseBerlinMODTS(ts: String): DateTime = {

    DateTime.parse(ts, formatMap(ts.length()))
  }

  val conffile = new File("application.conf")
  //log.info(conffile.getAbsolutePath())
  val conf = ConfigFactory.parseFile(conffile)

  def getConfig() = {
    conf
  }

  def getActorSystemConfig(host: String, port: String): Config = {

    val akkaThreads = 4
    val akkaBatchSize = 15
    val akkaTimeout = 100
    val akkaHeartBeatPauses = 600
    val akkaFailureDetector = 300.0
    val akkaHeartBeatInterval = 1000

    ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
      |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      """.stripMargin)

  }

  def createKafkaProducer() = {

    val conf = ConfigFactory.parseFile(new File("application.conf"))
    val props = new java.util.Properties()
    props.put("metadata.broker.list", conf.getString("kafka.producer.broker-list"))
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")

    val pconfig = new ProducerConfig(props)
    (new Producer[String, String](pconfig))
  }

  def createTrip(line: String): (MObject, MObject) = {
    val arr = line.split(",")
    // Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend      
    //    try {

    val moid = arr(0).toLong
    val tripid = arr(1)
    // 2007-05-28 08:41:15.434

    var tstart = parseBerlinMODTS(arr(2))

    val xstart = arr(4).toDouble
    val ystart = arr(5).toDouble

    // ignored for now
    //      val tend = arr(3)
    val xend = arr(6).toDouble
    val yend = arr(7).toDouble

    var tend = parseBerlinMODTS(arr(3))

    val ssobj = SObject(moid, xstart, ystart)
    val esobj = SObject(moid, xend, yend)

    (MObject(tstart, ssobj), MObject(tend, esobj))

    //    } catch {
    //      case e: NumberFormatException => {
    //        //e.printStackTrace()           
    //      }
    //    }
  }

  def createQueryPoint(qline: String): QueryPoint = {
    val arr = qline.split(",")
    QueryPoint(arr(0).toLong, Point2D(arr(1).toDouble, arr(2).toDouble))
  }

  def getGeneralizedRangeQuery(fs: Array[String], omarker: Int) = {
    var marker = omarker
    val firstPoint = fs(marker)
    var pointTemp = Helper.createQueryPoint(firstPoint)
    var pointList = ArrayBuffer[QueryPoint](pointTemp)

    var nextPoint = fs(marker + 1)
    var nextQPoint = Helper.createQueryPoint(nextPoint)

    while (pointTemp.qid == nextQPoint.qid) {
      pointList += nextQPoint
      marker += 1

      pointTemp = nextQPoint
      nextPoint = fs(marker + 1)
      nextQPoint = Helper.createQueryPoint(nextPoint)
    }

    val res = pointList.map(x => (x.qid, Seq(x.qpoint))).reduce((x1, x2) => (x1._1, x1._2 ++ x2._2))
    (marker, res)
  }

  def getSimpleRangeQuery(fs: Iterator[String], omarker: Int) = {
    var marker = omarker

    val firstPoint = fs.next()
    var pointTemp = Helper.createQueryPoint(firstPoint)
    var pointList = ArrayBuffer[QueryPoint](pointTemp)

    var nextPoint = fs.next()
    var nextQPoint = Helper.createQueryPoint(nextPoint)

    var maxXPoint = Helper.createQueryPoint(firstPoint)
    var maxYPoint = Helper.createQueryPoint(firstPoint)

    var minXPoint = Helper.createQueryPoint(firstPoint)
    var minYPoint = Helper.createQueryPoint(firstPoint)

    while (pointTemp.qid == nextQPoint.qid && fs.hasNext) {
      if (maxXPoint.qpoint.x < nextQPoint.qpoint.x) maxXPoint = nextQPoint
      if (maxYPoint.qpoint.y < nextQPoint.qpoint.y) maxYPoint = nextQPoint

      if (minXPoint.qpoint.x > nextQPoint.qpoint.x) minXPoint = nextQPoint
      if (minYPoint.qpoint.y > nextQPoint.qpoint.y) minYPoint = nextQPoint

      pointList += nextQPoint
      marker += 1

      pointTemp = nextQPoint
      nextPoint = fs.next()
      nextQPoint = Helper.createQueryPoint(nextPoint)
    }

    val qid = maxXPoint.qid
    val northeast = Point2D(maxXPoint.qpoint.x, maxYPoint.qpoint.y)
    val southwest = Point2D(minXPoint.qpoint.x, minYPoint.qpoint.y)

    val res = (qid, Array(northeast, southwest))
    (marker, res)
  }

  def createSSE(eventType: String, text: String): HttpData = {
    val sb = StringBuilder.newBuilder
    val curDate = org.joda.time.DateTime.now()
    sb.append(s"event: $eventType \n")
    sb.append("data: {\"time\": \" " + curDate + "\"}\n\n")
    sb.append(s"data: $text \n\n")

    HttpData(sb.toString())
  }
  
  def stringObject(in:String) :JsValue = {
   JsObject("content"-> JsString(in)) 
  }

  def selfString(in: String): JsValue = {
    JsString(in)
  }

  def formatSSETweets(in: String): JsValue = {
    val arr = in.split(",")
    JsObject("id" -> JsNumber(arr(0).toLong),
      "timestamp" -> JsString(arr(1)),
      "lat" -> JsNumber(arr(2).toDouble),
      "lng" -> JsNumber(arr(3).toDouble),
      "text" -> JsString(arr(4)))
  }

}

// Represents an MBR class
class MBR(val maxLat: Double, val minLong: Double, val minLat: Double, val maxLong: Double) {

  def contains(lat: Double, lng: Double): Boolean = {
    ((lat >= minLat) && (lat <= maxLat) && (lng >= minLong) && (lng <= maxLong))
  }

  def getQueryString(): String = {
    s"max_lat=$maxLat&max_long=$maxLong&min_lat=$minLat&min_long=$minLong"
  }

  def south = minLat
  def west = maxLong
  def north = maxLat
  def east = minLong

}

// Spray-json conversion protocol class, used for marshalling and unmashalling of JSON objects
object AppJsonProtocol extends DefaultJsonProtocol {

  implicit object MBRJsonFormat extends RootJsonFormat[MBR] {
    def write(c: MBR) = JsObject(
      "south" -> JsNumber(c.minLat),
      "east" -> JsNumber(c.minLong),
      "north" -> JsNumber(c.maxLat),
      "west" -> JsNumber(c.maxLong))

    def read(value: JsValue) = {
      value.asJsObject.getFields("south", "east", "north", "west") match {
        case Seq(JsNumber(minLat), JsNumber(maxLong), JsNumber(maxLat), JsNumber(minLong)) =>
          new MBR(minLat.toDouble, minLong.toDouble, maxLat.toDouble, maxLong.toDouble)
        case _ => throw new DeserializationException("MBR expected")
      }
    }
  }
}

object BerlinMODLatLngConverter {

  // The following values where obtained from BBBike <https://github.com/eserte/bbbike>
  val x0 = -780761.760862528
  val x1 = 67978.2421158527
  val x2 = -2285.59137120724
  val y0 = -5844741.03397902
  val y1 = 1214.24447469596
  val y2 = 111217.945663725

  def getLatLng(newx: Double, newy: Double) = {
    val lat = ((newx - x0) * y1 - (newy - y0) * x1) / (x2 * y1 - x1 * y2)
    val lng = ((newx - x0) * y2 - (newy - y0) * x2) / (x1 * y2 - y1 * x2)
    (lat, lng)
  }
}