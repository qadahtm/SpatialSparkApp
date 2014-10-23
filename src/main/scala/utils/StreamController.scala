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

