/*
 * Multi Predicate Query version 0.01
 * Ahmed S. Abdelhamid
 */

//package org.apache.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel





object Range extends Serializable {



  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Range <StreamServerIP> <port>")
      System.exit(2)
    }

    // Two Approaches
    // Define Two windows, for every item in first window join with the second window

    // Compute Euclidean Distance Between two tweets
    // First, Convert Long/Lat using BerlinMod Converter
    // Second, Use Euclidean distance equation:
    // D(p1, p2) = sqrt((x1-x2)^2 + (y1-y2)^2)

    // Textual Similarity:
    // For every word in the tweet,
    // Check Exact Match with all words in the other tweets

    val sparkConf = new SparkConf().setAppName("Range")

    // (0) Define a streaming Context with 1 second batch
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // (1) Listen to tweets stream source
    val tweetStream = ssc.socketTextStream(args(0), 2015, StorageLevel.MEMORY_ONLY)

    // (2) Define a window over the stream
    val window_size = Seconds(10)

    val tweets_window = tweetStream.window(window_size)

    val tweets_window2 = tweets_window

    // Temporal Range

    case class point(x:Double,y:Double) extends Serializable {
      override def toString() = {

        "X-Cor:" + x.toString()+" Y-Coordniate: "+y.toString()
      }
    }


    case class queryRegion(min:point,max:point) extends Serializable {
      override def toString() = {
        min+" "+max
      }

    }
    case class tweete(ID:Double, location:point, tText:Array[String],tweetL:Int, tTextL:Int, tTime:String) extends Serializable{

      override def equals(that:Any):Boolean = {
        true
      }

    }

    // Create a list for current tweets
    var tx: Set[tweete] = Set()


    var i =0
    // Parse Stream and Create Tweet tuple(s)
    val tweets = tweets_window.map(line => {
      val tweet = line.split(" ")
      val tID = tweet(0)
      val tDate = tweet(1)
      val tTime = tweet(2)
      val tLat = tweet(3)
      val tLong = tweet(4)
      var tText = tweet.slice(5, tweet.length)
      var location = point(0, 0)
      var ID = 0.0
      var Time:Long=0
      try {
        //Time = tTime.toLong
        location = point(tLat.toDouble, tLong.toDouble)
        ID = tID.toDouble

      }
      catch {
        case e: Exception => {
          Time = 0
          ID = 0.0
          location = point(0.0, 0.0)
          println("Format Exception is handled for point")
        }
      }

      val queryCandidate:tweete = new tweete(ID, location, tText, tweet.length, tText.length, tTime)
      val oneList:Set[tweete] = Set(queryCandidate)
      val tmp = tx
      tx = tmp.+(queryCandidate)
      i = i+1

      (1,queryCandidate)
    })
    var temp:StringBuilder = new StringBuilder()
    var temp2:StringBuilder = new StringBuilder()
    var d:Boolean = false
    val tweets2 = tweets.join(tweets).flatMap({
      case (_, (p, q)) => {

        if (q.ID != p.ID) {
          val part1 = (p.location.x.toDouble - q.location.x.toDouble) * (p.location.x.toDouble - q.location.x.toDouble)
          val part2 = (p.location.y.toDouble - q.location.y.toDouble) * (p.location.y.toDouble - q.location.y.toDouble)

          var Tdistance:Double = 0// p.tTime.toDouble - q.tTime.toDouble

          val distance = math.sqrt(part1 + part2)
          if (distance < 10) {

            p.tText.distinct.foreach(x => {
              for (y <- q.tText.distinct) {
                if (x.equals(y)){
                  temp.append(x)
                  temp.append(", ")
                }
              }
            })

            d = false
            if(!temp.isEmpty){
              temp2 = temp
              temp= new StringBuilder("")
              d = true

            }
            Some(p, q, "Spatial Distance=" + distance +  " Keyword Matches= " + temp2, d, Tdistance)

          }
          else
            None
        }
        else None
      }

    })




    tweets2.foreachRDD(t => t.foreach(f => {
      // Printing Tweet 1
      if(f._4) {
        println(Console.WHITE +"First Tweet")
        println("Tweet ID:" + f._1.ID.toLong + "  Tweet Length:" + f._1.tweetL + "  Text Length:" + f._1.tTextL)
        println("Tweet Long:" + f._1.location.x.toDouble + "  Tweet Lat:" + f._1.location.y.toDouble)
        //println("Tweet Time: " + f._1.tTime)
        print("TweetText:")
        for (x <- f._1.tText) {
          print(" " + x)
        }
        println()

        // Printing Tweet 2
        println(Console.WHITE + "Second Tweet")
        println("Tweet ID:" + f._2.ID.toLong + "  Tweet Length:" + f._2.tweetL + "  Text Length:" + f._2.tTextL)
        println("Tweet Long:" + f._2.location.x.toDouble + "  Tweet Lat:" + f._2.location.y.toDouble)
        //println("Tweet Time: " + f._2.tTime)
        print("TweetText:")
        for (y <- f._2.tText) {
          print(" " + y)
        }
        println()

        print(Console.BOLD + "Matching Details: ")
        println(f._3)

        println(Console.WHITE + "===========================================")
        println()

      }

    }))




    ssc.start()
    ssc.awaitTermination()
  }
}


