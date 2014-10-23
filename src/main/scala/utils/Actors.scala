package utils

import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorLogging
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.javaapi.producer.Producer
import akka.actor.Actor
import akka.actor.PoisonPill
import scala.concurrent.duration._
import akka.actor.ActorPath
import scala.reflect.ClassTag
import org.apache.spark.streaming.receiver.ActorHelper
import akka.actor.ActorRef
import scala.concurrent.Await
import akka.actor.Terminated

case class Subscribe(ap: AnyRef)
case class Unsubscribe(ap: AnyRef)

case class Forward[T](val msg: T)

class SparkSampleActorReceiver[T: ClassTag](urlOfPublisher: String)
  extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart = remotePublisher ! Subscribe(context.self)

  def receive = {
    case msg => {
      store(msg.asInstanceOf[T])
    }
  }

  override def postStop = remotePublisher ! Unsubscribe(context.self)

  
}

class PubSubActor[T] extends Actor with ActorLogging {

  val localSubs = ArrayBuffer[ActorRef]()
  val remoteSubs = ArrayBuffer[String]()

  def receive = {
    case Subscribe(ap) => {
      
      ap match {
        case x: String => {
          log.info("Subscribing Remote actor: " + ap)
          val timeout = 100 seconds

          val f = context.actorSelection(x).resolveOne(timeout).mapTo[ActorRef]
          val aref = Await.result(f, timeout)
          this.context.watch(aref)

          remoteSubs += x

        }
        case x: ActorRef => {
          log.info("Subscribing Local actor: " + ap)
          this.context.watch(x)
          localSubs += x

        }
      }

    }
    case Unsubscribe(ap) => {
      
      //      subscribers.dropWhile(_.eq(ap))
      ap match {
        case x: String => {
          log.info("UnSubscribing Remote: " + ap)
          remoteSubs -= x
        }
        case x: ActorRef => {
          log.info("UnSubscribing local: " + ap)
          localSubs -= x
        }
      }

    }
    case x: Forward[T] => {
      //      log.info("Forwarding a message : "+x.msg)      
      if (localSubs.size > 0) {
        localSubs.foreach(s => {
          s ! x.msg
        })
      }

      if (remoteSubs.size > 0) {
        //TODO : resolve remote reference first
        remoteSubs.foreach(s => {
          this.context.actorSelection(s) ! x.msg
        })
      }
    }

    case Terminated(t) => {
      
      // Works for local subscribers 
      // If local subscribers are terminated without sending an UnSubscribe message
      t match {
        case x:ActorRef =>{
        	localSubs -= x
        }
      }
      
      
      
      log.info("Ther actor : "+t+" is terminated")
    }
    
    case msg => {
      log.error("received a message of type : "+msg.getClass.getName())
    }
  }
}

class PrinterActor extends Actor with ActorLogging {

  log.info("Printer start")

  def receive = {
    case x: Any => {
      log.info(x.toString)
    }
  }
}

class BerlinMODQueryController(filepath: String, count: Int, topic: String) extends Actor with ActorLogging {

  implicit val ec = context.system.dispatcher

  lazy val fs = scala.io.Source.fromFile(filepath).getLines

  val props = new java.util.Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "example.producer.SimplePartitioner")
  props.put("request.required.acks", "1")

  val pconfig = new ProducerConfig(props)
  val producer = new Producer[String, String](pconfig)

  var marker = 1
  // skipping the first line
  fs.next()
  var qcount = 1
  val maxcount = 10

  def receive = {
    case "sendout" => {
      //      log.info("sending out")
      val queries = ArrayBuffer[String]()

      if (fs.hasNext) {
        for (i <- 1 to count) {
          val q = Helper.getSimpleRangeQuery(fs, marker);
          val res = q._2._1 + "," + q._2._2.mkString(",")

          val data = new KeyedMessage[String, String](topic, res)
          producer.send(data)

          queries += res
          //        log.info(res.toString)
          marker = q._1 + 1
        }
        qcount = qcount + 1
        context.system.scheduler.scheduleOnce(1 seconds) { self ! "sendout" }
      } else {
        log.info("done and not sending anymore")
        self ! PoisonPill
      }
    }
    case _ => log.info("got something")
  }

}