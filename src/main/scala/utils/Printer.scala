package utils

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import scala.concurrent.Await
import java.util.logging.Logger
import akka.actor.ActorPath
import akka.actor.Address

object Printer extends App {

  val log = Logger.getLogger(this.getClass().getName())

  val casys = "printer"
  val chost = "0.0.0.0"
  val cport = 9997
  val cname = "printer"

  val conf = Helper.getActorSystemConfig(chost, cport.toString)

  val asys = ActorSystem(casys, conf)

  val p = Props(classOf[PrinterActor])
  val a = asys.actorOf(p, cname)

  if (args.length > 0) {
    args(0) match {
      case "-pubsub" => {
        val asysname = args(1)
        val host = args(2)
        val port = args(3)
        val name = args(4)
        import scala.concurrent.duration._
        val timeout = 100 seconds

        val url = s"akka.tcp://$asysname@$host:$port/user/$name"
        val pubsub = Await.result(asys.actorSelection(url).resolveOne(timeout), timeout)

        log.info("Sending subscribe to : " + pubsub.path.toString())

        log.info("sending path : " + a.path.toString())

        val remotePath = Address("akka.tcp", casys, host, port.toInt)
        val purl = s"akka.tcp://$casys@$chost:$cport/user/$cname"

        sys addShutdownHook {
          println("Shutdown hook caught.")
          pubsub ! Unsubscribe(purl)
          println("Done shutting down.")
        }
        pubsub ! Subscribe(purl)
      }
      case _ => {
        println("Error, use the following arguments: -pubsub actorSysName host port actorname")
      }
    }
  }

  asys.awaitTermination

}