package queries;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import helpers.LocationUpdate;

import java.util.ArrayList;
import java.util.List;

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
  private static JavaPairDStream<Integer, List<KNNQuery>> totalqueries;

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: IncrementalKNNQuery <hostname> <tupleport> <queryport>");
      System.exit(1);
    }

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("IncrementalKNNQuery");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,  new Duration(1000));


    //Input DStreams that connect to a input and query stream
    JavaReceiverInputDStream<String> queries = ssc.socketTextStream(
            args[0], Integer.parseInt(args[2]), StorageLevels.MEMORY_AND_DISK_SER);
    
    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
    
    ssc.checkpoint("~/spark-checkpoint/");
    
    final JavaDStream<TweetRecord> tuples;
    final JavaDStream<KNNQuery> queryTuples;
    

    //create an DStream that contains all queries
    queryTuples = queries.map(new Function<String, KNNQuery>() {
       public KNNQuery call(String line) throws Exception {
 
         String[] fields = line.split(",");
         
         KNNQuery q = new KNNQuery(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), 
        		 Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
         
         return q;
       }
     });
    
    //create a DStream that contains all tuples
    tuples = lines.map(new Function<String, TweetRecord>() {
    	public TweetRecord call(String line) throws Exception {
 
         String[] fields = line.split(",",6);

         TweetRecord sd = new TweetRecord(
        		 Long.parseLong(fields[0]), 
        		 fields[1], 
        		 Double.parseDouble(fields[2]),  
        		 Double.parseDouble(fields[3]), 
        		 Long.parseLong(fields[4]),
        		 fields[5]);
         return sd;
       }
     });
    
    //TODO:reduce for queries with same id
    
    
    //First map (1, TweetRecord) for each tweet
    JavaPairDStream<Integer, TweetRecord> tupleRDD = 
    		tuples.mapToPair(new PairFunction<TweetRecord, Integer, TweetRecord>(){
    		    public Tuple2<Integer, TweetRecord> call(TweetRecord record){
    		      
    		    Tuple2<Integer,TweetRecord> t2 = new Tuple2<Integer,TweetRecord>(1, record);
    		      return t2;
    		    }
    		    
    		});
    
    //Then map (1, KNNQuery) for each query
    JavaPairDStream<Integer, KNNQuery> queryRDD = 
    		queryTuples.mapToPair(new PairFunction<KNNQuery, Integer, KNNQuery>(){
    		    public Tuple2<Integer, KNNQuery> call(KNNQuery q){
    		      
    		    Tuple2<Integer,KNNQuery> t2 = new Tuple2<Integer,KNNQuery>(1, q);
    		      return t2;
    		    }
    		    
    		});
    
    //Function to retain state of total queries
    Function2<List<KNNQuery>, Optional<List<KNNQuery>>, Optional<List<KNNQuery>>> updateFunction =
    		  new Function2<List<KNNQuery>, Optional<List<KNNQuery>>, Optional<List<KNNQuery>>>() {
    		    @Override 
    		    public Optional<List<KNNQuery>> call(List<KNNQuery> values, Optional<List<KNNQuery>> state) {
    		    	List<KNNQuery> newState = new ArrayList<KNNQuery>();

    		    	newState.addAll(values);
    		    	
    		    	if(state.isPresent()){
    		    		for(KNNQuery q:state.get()){
    		    			newState.add(q);
    		    		}
    		    	}
    		    	
    		      return Optional.of(newState);
    		    }
    		  };
    
    //Add new queries to total queries
  	totalqueries = queryRDD.updateStateByKey(updateFunction);
    
  	//join queries by id for now print..later output will be (Query_ID,List<TweetRecord> topKNNs) 
    tupleRDD.join(totalqueries).foreachRDD(
    		new Function<JavaPairRDD<Integer,Tuple2<TweetRecord,List<KNNQuery>>>, Void>() {

				@Override
				public Void call(JavaPairRDD<Integer, Tuple2<TweetRecord, List<KNNQuery>>> joinRDD)
						throws Exception {
						for(Tuple2<Integer, Tuple2<TweetRecord,List<KNNQuery>>> tup:joinRDD.collect()){
						TweetRecord tr = tup._2._1;
						List<KNNQuery> queries = tup._2._2;
						for(KNNQuery q: queries){
							LocationUpdate locationUpdate = new LocationUpdate(tr.getUserId(),(int)tr.getGeoLat(), (int)tr.getGeoLong());
							ArrayList<String> changes = q.processLocationUpdate(locationUpdate);
								for (Integer str : q.getCurrentRanks()) {
									System.out.println(str);
								}
						}
					}
					return null;
				}
	});


    ssc.start();
    ssc.awaitTermination();
  }
  
}
