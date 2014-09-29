package utils

import akka.actor._
import akka.event._
import akka.dispatch._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent._
import scala.concurrent.duration._
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import scala.collection.mutable.ArrayBuffer
import java.util.logging.Logger

object StreamController extends App {
  
//  this.args.foreach(s => Console.println(s))
  val filepath = this.args(0)
  val topic = args(3)
  val count = this.args(1).toInt
  val period = this.args(2).toInt
  
  
  val rate = count/period
  
  
  
  if (this.args.length != 4){
    Console.println("Incorrect number of parameters: filePath count period topic")
    
  }
  else{
    
	  println(s"Rate = $rate per $period seconds")
	  implicit val system = ActorSystem("stream-controller")
	  implicit val ec = system.dispatcher
	  implicit val timeout = Timeout(3)
	 
	  val c = system.actorOf(Props(classOf[Controller], filepath,count, topic),name="controller")
	  
	  system.scheduler.schedule(1 seconds, period seconds) {
	  c ! "sendout"	
      }
	   
  }
  
  

}

class Controller(filepath:String,count:Int, topic:String) extends Actor with ActorLogging {
  
  lazy val fs = scala.io.Source.fromFile(filepath).getLines.toArray[String]
  
    val props = new java.util.Properties()
	props.put("metadata.broker.list", "localhost:9092")
	props.put("serializer.class", "kafka.serializer.StringEncoder")
	//props.put("partitioner.class", "example.producer.SimplePartitioner")
	props.put("request.required.acks", "1")
 
	val pconfig = new ProducerConfig(props)
	val producer = new Producer[String, String](pconfig)
  
  var marker = 1 // skipping hte first line
  
  def receive = {
    case "sendout" => {
//      log.info("sending out")
      
      val res = fs(marker)
      
      for (i <- 1 to count){      
        val data = new KeyedMessage[String, String](topic, res)
        producer.send(data)
        
//        log.info(data.toString)
        marker = marker +1
      }
  	}
    case _ => log.info("got something")
  }
  
}

object QueryStreamController extends App {
  
//  this.args.foreach(s => Console.println(s))
  val filepath = this.args(0)
  val topic = args(3)
  val count = this.args(1).toInt
  val period = this.args(2).toInt
  
  
  val rate = count/period
  
  
  
  if (this.args.length != 4){
    Console.println("Incorrect number of parameters: filePath count period topic")
    
  }
  else{
    
	  println(s"Rate = $rate per $period seconds")
	  implicit val system = ActorSystem("stream-controller")
	  implicit val ec = system.dispatcher
	  implicit val timeout = Timeout(3)
	 
	  val c = system.actorOf(Props(classOf[BerlinMODQueryController], filepath,count, topic),name="controller")
	  
	  system.scheduler.scheduleOnce(1 seconds) { c ! "sendout" }
	   
  }
  
  

}


case class Point2D(x:Double,y:Double) extends Serializable {
  override def toString() = {
    val latlng = BerlinMODLatLngConverter.getLatLng(x, y)    
    latlng._1+":"+latlng._2
  }
}
case class QueryPoint(qid:Long,qpoint: Point2D) extends Serializable

object Helper {
  def createQueryPoint(qline:String) :QueryPoint = {
	  val arr = qline.split(",")
	  QueryPoint(arr(0).toLong, Point2D(arr(1).toDouble,arr(2).toDouble))
  }
  
  def getGeneralizedRangeQuery(fs:Array[String], omarker:Int) = {
    var marker = omarker
    val firstPoint = fs(marker)
      var pointTemp = Helper.createQueryPoint(firstPoint)
      var pointList = ArrayBuffer[QueryPoint](pointTemp)
      
      var nextPoint = fs(marker+1)
      var nextQPoint = Helper.createQueryPoint(nextPoint)
      
      while (pointTemp.qid == nextQPoint.qid){
        pointList += nextQPoint
		marker += 1
        
		pointTemp = nextQPoint
		nextPoint = fs(marker+1)
      	nextQPoint = Helper.createQueryPoint(nextPoint)		
      }
      
      
      val res = pointList.map(x => (x.qid,Seq(x.qpoint))).reduce((x1,x2) => (x1._1,x1._2 ++ x2._2))
      (marker, res)
  }
  
  def getSimpleRangeQuery(fs:Array[String], omarker:Int) = {
    var marker = omarker
    val firstPoint = fs(marker)
      var pointTemp = Helper.createQueryPoint(firstPoint)
      var pointList = ArrayBuffer[QueryPoint](pointTemp)
      
      var nextPoint = fs(marker+1)
      var nextQPoint = Helper.createQueryPoint(nextPoint)
      
      var maxXPoint = Helper.createQueryPoint(firstPoint)
      var maxYPoint = Helper.createQueryPoint(firstPoint)
      
      var minXPoint = Helper.createQueryPoint(firstPoint)
      var minYPoint = Helper.createQueryPoint(firstPoint)
      
      while (pointTemp.qid == nextQPoint.qid){
        if (maxXPoint.qpoint.x <  nextQPoint.qpoint.x) maxXPoint = nextQPoint
        if (maxYPoint.qpoint.y < nextQPoint.qpoint.y) maxYPoint = nextQPoint
        
        if (minXPoint.qpoint.x > nextQPoint.qpoint.x) minXPoint = nextQPoint
        if (minYPoint.qpoint.y > nextQPoint.qpoint.y) minYPoint = nextQPoint
        
        pointList += nextQPoint
		marker += 1
        
		pointTemp = nextQPoint
		nextPoint = fs(marker+1)
      	nextQPoint = Helper.createQueryPoint(nextPoint)		
      }
    
    
//      val xs = pointList.map(_.qpoint.x).sorted
//      val ys = pointList.map(_.qpoint.y).sorted
//      
//      val log = Logger.getLogger(this.getClass().getName())      
//      log.info("for q = "+pointTemp.qid+" maxx, "+xs.last +" = "+maxXPoint.qpoint.x)
//      log.info("for q = "+pointTemp.qid+" maxy, "+ys.last +" = "+maxYPoint.qpoint.y)
//      log.info("for q = "+pointTemp.qid+" minx, "+xs.head +" = "+minXPoint.qpoint.x)
//      log.info("for q = "+pointTemp.qid+" miny, "+ys.head +" = "+minYPoint.qpoint.y)
      
      val qid = maxXPoint.qid
      val northwest = Point2D(minXPoint.qpoint.x,maxYPoint.qpoint.y)         
      val southeast = Point2D(maxXPoint.qpoint.x,minYPoint.qpoint.y) 
      
      val res = (qid,Array(northwest,southeast))
      (marker, res)
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
       
   def getLatLng(newx:Double, newy:Double) = {
    val lat = ((newx-x0)*y1-(newy-y0)*x1)/(x2*y1-x1*y2) 
    val lng = ((newx-x0)*y2-(newy-y0)*x2)/(x1*y2-y1*x2)
    (lat,lng)  
  }
}

class BerlinMODQueryController(filepath:String,count:Int, topic:String) extends Actor with ActorLogging {
  
  implicit val ec = context.system.dispatcher
  
  lazy val fs = scala.io.Source.fromFile(filepath).getLines.toArray[String]
  
    val props = new java.util.Properties()
	props.put("metadata.broker.list", "localhost:9092")
	props.put("serializer.class", "kafka.serializer.StringEncoder")
	//props.put("partitioner.class", "example.producer.SimplePartitioner")
	props.put("request.required.acks", "1")
 
	val pconfig = new ProducerConfig(props)
	val producer = new Producer[String, String](pconfig)
  
  var marker = 1 // skipping hte first line
  var qcount = 1
  val maxcount = 10
  
  
  
  def receive = {
    case "sendout" => {
//      log.info("sending out")
      val queries = ArrayBuffer[String]()
      
      if (qcount != maxcount) {
        for (i <- 1 to count){
	        val q = Helper.getSimpleRangeQuery(fs, marker);
	        val res = q._2._1+","+q._2._2.mkString(",")
	        
	        val data = new KeyedMessage[String, String](topic, res)
	        producer.send(data)
	        queries += res
	//        log.info(res.toString)
	        marker = q._1 + 1
	      }
		  qcount = qcount +1   
		  context.system.scheduler.scheduleOnce(1 seconds) { self ! "sendout" }
      }
      else{
    	  log.info("done and not sending anymore")
    	  self ! PoisonPill
      }            
  	}
    case _ => log.info("got something")
  }
  
}