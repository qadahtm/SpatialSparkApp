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

object KafkaActorGroupConsumer extends App {
  
  val sys = ActorSystem("kafkaConsumer")
  
  val zk = "localhost:2182"
  val cgid = "cgid"
  val topic = "output"
  
  val conf = createConsumerConfig(zk, cgid)
  val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
  val topicCountMap = Map(topic -> 1.asInstanceOf[Integer]).asJava
  
  val consumerMap = consumer.createMessageStreams(topicCountMap,new StringDecoder(), new StringDecoder())
  val streams = consumerMap.get(topic)
  
  val aprops = Props(classOf[KafkaStringConsumer], streams.get(0), 0)
  val c = sys.actorOf(aprops)
  
  c ! "checkMsgs"
  
  
  def createConsumerConfig( a_zookeeper: String,  a_groupId: String) : ConsumerConfig = {
        val props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        (new ConsumerConfig(props))
   }
  
}

class KafkaStringConsumer(val stream : KafkaStream[String,String], val threadNum : Int) extends Actor with ActorLogging {
    implicit val ec = this.context.system.dispatcher
    def receive = {
      case "checkMsgs" => {
        val f = Future {
          stream.iterator.foreach(x => {
	          log.info("KafkaMessage "+threadNum+" : "+x.message)
	        })
        }
      }
      case _ => log.info("got a message")
    }
  }