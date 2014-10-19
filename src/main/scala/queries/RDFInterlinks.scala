package queries

import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
 * Created by Amgad Madkour on 10/2/14.
 */

object RDFInterlinks {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 2) {
      System.err.println("Usage: RDFInterlinks  <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("RDFInterlinks")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    val res = lines.map(line => {

      val x = line.split("\t")
      val endpoint = "http://ibnalhaytham.cs.purdue.edu:3030/dbpedia/sparql"
      //val endpoint = "http://dbpedia.org/sparql"

      val querystring = "select ?label " +
        "                where { <"+x(2)+"> <http://www.w3.org/2000/01/rdf-schema#label> ?label ."+
        "                        <"+x(2)+"> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Settlement> ." +
        "                        FILTER (LANG(?label)='en')"+
        "                      }"


      val qe = new QueryEngineHTTP(endpoint, querystring)
      val result = qe.execSelect()

      var thing = ""

      while (result.hasNext()) {
        val row= result.next()
        val obj= row.get("label")
        thing = obj.toString
      }

      (thing)

    })

    //Print the content of the DStream
    val fl = res.filter(line => !line.toString.isEmpty)

    fl.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

