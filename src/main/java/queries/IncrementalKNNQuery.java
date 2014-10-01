package queries;

import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import helpers.LocationUpdate;

import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import kafka.utils.Time;
import queries.knn.*;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 * Usage: IncrementalKNNQuery <hostname> <port>
 *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example java.queries.IncrementalKNNQuery localhost 9999`
 */
public final class IncrementalKNNQuery {
  private static ArrayList<KNNQuery> queryList;


  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: IncrementalKNNQuery <hostname> <port>");
      System.exit(1);
    }

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("IncrementalKNNQuery");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,  new Duration(1000));
    queryList = new ArrayList<KNNQuery>();
    //add query statically for now and implement query stream later
    queryList.add(new KNNQuery(0, 4, 5, 2));
    queryList.add(new KNNQuery(1, 9, 10, 2));
    queryList.add(new KNNQuery(2, 15, 16, 2));

    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);

    //create an RDD that contains all tuples
    JavaDStream<TweetRecord> tuples = lines.map(
  	new Function<String, TweetRecord>() {
       public TweetRecord call(String line) throws Exception {
 
         String[] fields = line.split(",");
         TweetRecord sd = new TweetRecord(Integer.parseInt(fields[0]),fields[1], Double.parseDouble(fields[2]), 
        		 Double.parseDouble(fields[3]), fields[4].trim(), fields[5].trim(), Integer.parseInt(fields[6]),Integer.parseInt(fields[7]));
         return sd;
       }
     });
    
//    //First map (user_id, (TweetRecord, Date)) --> ReduceByMaxDate() --> (userID, (lat,long))
//    JavaPairDStream<Integer, Tuple2<TweetRecord,Date>> dateRDD = 
//    		tuples.mapToPair(new PairFunction<TweetRecord, Integer, Tuple2<TweetRecord,Date>>(){
//    		    public Tuple2<Integer, Tuple2<TweetRecord,Date>> call(TweetRecord record){
//    		      
//    		    Tuple2<Integer, Tuple2<TweetRecord,Date>> t2 = new Tuple2<Integer, Tuple2<TweetRecord,Date>>(record.getUserId(),
//    		    		new Tuple2<TweetRecord,Date>(record, record.getDateCreatedAt()));
//    		      return t2;
//    		    }
//    		    
//    		});
 

//  // map (user_id, (geolat, geolong)) 
//    JavaPairDStream<Integer, Tuple2<Double, Double>> locationRDD = 
//    		tuples.mapToPair(new PairFunction<TweetRecord, Integer, Tuple2<Double, Double>>(){
//    		    public Tuple2<Integer, Tuple2<Double, Double>> call(TweetRecord record){
//    		      
//    		    Tuple2<Integer, Tuple2<Double, Double>> t2 = new Tuple2<Integer, Tuple2<Double, Double>>(record.getUserId(),
//    		    		new Tuple2<Double, Double>(record.getGeoLat(), record.getGeoLong()));
//    		      return t2;
//    		    }
//    		    
//    		});
    
    //stupid way of doing this for now
    tuples.foreachRDD(new Function<JavaRDD<TweetRecord>, Void>(){
    	public Void call(JavaRDD<TweetRecord> recordRDD){
    		List<TweetRecord> tweetTuples = recordRDD.collect();
    		for(TweetRecord tr: tweetTuples){
    			filterData(new LocationUpdate(tr.getTweetId(),(int)tr.getGeoLat(), (int)tr.getGeoLong()));
    		}
    		return null;
    	}
    	
    });
    
    
//    JavaPairDStream<String, Tuple2<Long, Integer>> kNN = 
//    		locationRDD.reduceByKey(new Function2<Tuple2<Long, Integer>, Tuple2<Long,
//    		 Integer>, Tuple2<Long, Integer>>() {
//    		    public Tuple2<Long, Integer> call(Tuple2<Long, Integer> v1,
//    		    Tuple2<Long, Integer> v2) throws Exception {
//    		        return new Tuple2<Long, Integer>(v1._1 + v2._1, v1._2+ v2._2);
//    		    }
//    		});


    ssc.start();
    ssc.awaitTermination();
  }

 /* public void execute(Tuple4 input) {
	if (Constants.objectLocationGenerator.equals(input.getSourceComponent())) {
		filterData(input,collector);
	} else if (Constants.queryGenerator.equals(input.getSourceComponent())) {
		addQuery(input);
	}
  }

   void addQuery(Tuple4 input) {
	KNNQuery query = new KNNQuery(input.getIntegerByField(Constants.queryIdField),
			input.getIntegerByField(Constants.focalXCoordField),
			input.getIntegerByField(Constants.focalYCoordField),
			input.getIntegerByField(Constants.kField));
	queryList.add(query);
   }*/

  static void filterData(LocationUpdate locationUpdate) {
	for (KNNQuery q : queryList) {
		ArrayList<String> changes = q.processLocationUpdate(locationUpdate);
		for (String str : changes) {
			if (str.charAt(0) == '-')
				System.err.println(str);
			else
				System.out.println(str);
		}
	}

  }//end filterData

   static void printChanges(ArrayList<String> changes){
	  for (String str : changes) {
	  	if (str.charAt(0) == '-')
			System.err.println(str);
		else
			System.out.println(str);
	  }
   }

}
