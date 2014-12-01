/**
 * Created by Amgad Madkour on 10/23/14.
 */


import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream


object MultiplePredicates {

  case class Tweet(tweet_id: String, created_at: String, geo_lat:Double, geo_long:Double, user_id:String, tweet_text: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("MultiplePredicates")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val streamSqlContext = new StreamSQLContext(ssc)

    // Create an DStream of Person objects and register it as a stream.
    val tweets: DStream[Tweet] = ssc.socketTextStream("localhost", 1234).map(_.split(",")).map(p => Tweet(p(0), p(1),p(2).toDouble,p(3).toDouble,p(4),p(5)))
    val x  = streamSqlContext.createSchemaDStream(tweets)
    x.registerAsStream("tweets")

    //Spatial: California
    //Textual: Tweet Containing the Keyword "game"
    //Temporal: Date Not Supported Yet

    val filtered = streamSqlContext.streamSql(
      "SELECT " +
        "geo_lat,geo_long,tweet_text " +
        "FROM tweets " +
        "WHERE " +
//        "tweet_text LIKE '%game%' AND " +
        "((41.952405 >= geo_lat AND geo_lat >= 32.788501) AND (-114.038086 >= geo_long AND geo_long >= -124.233398))" +
        "")

    // The results of SQL queries are themselves DStreams and support all the normal operations
    filtered.map(t => "\nLat: " + t(0) +"\n"+ "Long: "+t(1)+"\n"+"Text: "+t(2)+"Expiration: "+ 5).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}


