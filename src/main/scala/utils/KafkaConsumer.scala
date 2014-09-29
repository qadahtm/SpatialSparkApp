package utils
import akka.actor.{Props, ActorSystem, Actor}
import com.sclasen.akka.kafka.{AkkaConsumer, AkkaConsumerProps, StreamFSM}
import kafka.serializer._

object KafkaConsumer extends App{
  class Printer extends Actor{
    def receive = {
      case x:Any =>
        println(x.toString)
        println(x.getClass.getName())
        sender ! StreamFSM.Processed
    }
  }

  val system = ActorSystem("test")
  val printer = system.actorOf(Props[Printer])


  /*
  the consumer will have 4 streams and max 64 messages per stream in flight, for a total of 256
  concurrently processed messages.
  */
  val consumerProps = AkkaConsumerProps.forSystem(
    system = system,
    zkConnect = "localhost:2182",
    topic = "input",
    group = "ui-consumer-group",
    streams = 1, //one per partition
    keyDecoder = new StringDecoder(),
    msgDecoder = new StringDecoder(),
    receiver = printer
  )

  val consumer = new AkkaConsumer(consumerProps)

  consumer.start()  //returns a Future[Unit] that completes when the connector is started

  consumer.commit() //returns a Future[Unit] that completes when all in-flight messages are processed and offsets are committed.

 // consumer.stop()   //returns a Future[Unit] that completes when the connector is stopped.

}