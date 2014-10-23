package utils

import akka.actor.Actor
import akka.actor.ActorLogging
import kafka.consumer.KafkaStream
import scala.concurrent.Future
import akka.actor.ActorSystem
import kafka.consumer.ConsumerConfig
import java.util.Properties
import java.util.HashMap
import scala.collection.JavaConverters._
import akka.actor.Props
import kafka.serializer.StringDecoder
import akka.actor.ActorRef

object KafkaActorGroupConsumer extends App {

  val sys = ActorSystem("kafkaConsumer")
  val printer = sys.actorOf(Props[Printer])
  
  val zk = "localhost:2182"
  val cgid = "cgid"
  val topic = "output"
    
  KafkaConsumerHelper.startKafkaConsumer(zk,cgid,topic, sys, printer)

  class Printer extends Actor with ActorLogging{
    def receive = {
      case KafkaStringMessage(x) =>
        log.info("received from kafka : "+x)
    }
  }

  
}

object KafkaConsumerHelper {

  def startKafkaConsumer(zk: String, cgid: String, topic: String, sys: ActorSystem, target: ActorRef) = {
    val conf = KafkaConsumerHelper.createConsumerConfig(zk, cgid)
    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
    val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava

    val consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(), new StringDecoder())
    val streams = consumerMap.get(topic)

    val aprops = Props(classOf[KafkaStringConsumer], streams.get(0), target)
    val c = sys.actorOf(aprops)

    c ! "checkMsgs"
    c
  }

  def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props = new Properties();
    props.put("zookeeper.connect", a_zookeeper);
    props.put("group.id", a_groupId);
    props.put("zookeeper.session.timeout.ms", "1000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    (new ConsumerConfig(props))
  }

}

case class KafkaStringMessage(val x: String) extends Serializable

class KafkaStringConsumer(val stream: KafkaStream[String, String], val target: ActorRef) extends Actor with ActorLogging {
  implicit val ec = this.context.system.dispatcher
  def receive = {
    case "checkMsgs" => {
      val f = Future {
        stream.iterator.foreach(x => {
          //          log.info("KafkaMessage " + threadNum + " : " + x.message)
          target ! (new KafkaStringMessage(x.message))
        })
      }
    }
    case _ => log.info("got a message")
  }
}