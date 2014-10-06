package queries

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import akka.japi.Option.Some

object Agg extends Logging{
  def main(args: Array[String]) {
    if (args.length < 7) {
      // if incremental == 0, then aggregate for the batch or window
      // if incremental == 1, then aggregate from the landmark
      // <cmd>: "-count", "-max", "-min", "-avg", "-sum", current support aggregation operators
      System.err.println("Usage: Agg-Window <cmd> <hostname> <port> <batch_size> <window_size> <incremental> <x1,y1,x2,y2>")
      System.exit(1)
    }
    setStreamingLogLevels()
    args(0) match {
      case "-count" => aggCount(args)
      
      case "-max" => aggMax(args)
      
      case "-min" => aggMin(args)
      
      case "-avg" => aggAvg(args)
      
      case "-sum" => aggSum(args)
      
      case "-help" => println("supported aggregations:\n -count, -max, -min, -avg, -sum"); System.exit(1);
      
      case _ => println("Operator not supported!"); System.exit(1);
    }
  }
	  
  def aggCount(arg: Array[String]) {
      val sparkConf = new SparkConf().setAppName("Agg-Count")
      val ssc = new StreamingContext(sparkConf, Seconds(arg(3).toInt))
    
      val updateCnt = (values: Seq[Long], state: Option[Long]) => {
          val currCount = values.sum
      
          val preCount = state.getOrElse(0L)
       
          Option(currCount + preCount)
      }
      
      val queryRec = getPoints(arg(6))

      val src = ssc.socketTextStream(arg(1), arg(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
      ssc.checkpoint("./spark-checkpoint/")
      
      // truncate the latitude and longitude to the first 8 digits     
      val test = src.map(_.split(" ")).filter(x => {
        if (x.length < 6) {
          false
        }
        else {
          true
        }
      })
      
      val pos = test.filter(x => isAllDigits(x(0))).map(x => (x(3).slice(0, 8).toDouble, x(4).slice(0, 8).toDouble))
          
      pos.print()
          
      val filterP1 = filterFunc(queryRec(0))(queryRec(1))_
 
      val queryCnt = pos.filter(filterP1(_))
      
      if (arg(5).toInt == 0) {
        if (arg(4).toInt == 0) {
          val cnt = queryCnt.count()
          cnt.print()
        }
        else {
          val cnt = queryCnt.countByWindow(Seconds(arg(4).toInt), Seconds(arg(4).toInt))
          cnt.print()
        }
      }
      else {
        if (arg(4).toInt == 0) {
          val cnt = queryCnt.count()
          val pairs = cnt.map(n => ("aggBatchCnt", n))
          val aggCnt = pairs.updateStateByKey[Long](updateCnt)
          aggCnt.print()
        }
        else {
          val cnt = queryCnt.countByWindow(Seconds(arg(4).toInt), Seconds(arg(4).toInt))
          val pairs = cnt.map(n => ("aggWindowCnt", n))
          val aggCnt = pairs.updateStateByKey[Long](updateCnt)
          aggCnt.print()
          //val output = aggCnt.map(x => x._2)
          //output.print()
        }
      }
    
      ssc.start()
      ssc.awaitTermination()
  }

  
  def filterFunc(queryP1: (Double, Double)) (queryP2: (Double, Double)) ( point: (Double, Double)): Boolean = {
    val cond1 = (queryP1._1 <= point._1) && (point._1 <= queryP2._1)
    val cond2 = (queryP2._1 <= point._1) && (point._1 <= queryP1._1)
    val cond3 = (queryP1._2 <= point._2) && (point._2 <= queryP1._2)
    val cond4 = (queryP2._2 <= point._2) && (point._2 <= queryP1._2)
    (cond1 || cond2) && (cond3 || cond4)
  }
  
  def aggMax(arg: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Agg-Max")
    val ssc = new StreamingContext(sparkConf, Seconds(arg(3).toInt))
    
    val updateMax = (values: Seq[Double], state: Option[Double]) => {
      var currMax = Double.MinValue
      
      if (values.length > 0) {
        currMax = values.max
      }
      
      //val currMax = values.max
      
      val preMax = state.getOrElse(Double.MinValue)
      
      Option(math.max(currMax, preMax))
    }
    
    val queryRec = getPoints(arg(6))
    
    val src = ssc.socketTextStream(arg(1), arg(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    ssc.checkpoint("./spark-checkpoint/")
    
    // get the valid records first
    val test = src.map(_.split(" ")).filter(x => {
        if (x.length < 6) {
          false
        }
        else {
          true
        }
      })
      
    val pos = test.filter(x => isAllDigits(x(0))).map(x => (x(3).slice(0, 8).toDouble, x(4).slice(0, 8).toDouble))
    // TODO: need to further extract proper fields from records to proceed
    // currently using latitude value for testing purpose
    pos.print()  // print first 10 records of the latitude values
    
    val filterP1 = filterFunc(queryRec(0))(queryRec(1))_
   
    val queryMax= pos.filter(filterP1(_)).map(x => x._1)
    
    if (arg(4).toInt == 0) {
      println("Window size cannot be 0 for max()")
      System.exit(1)
    }
    else {
      val streamMax = queryMax.reduceByWindow(math.max, Seconds(arg(4).toInt), Seconds(arg(4).toInt))
      if (arg(5).toInt == 0) { // only count current window        
        streamMax.print()
      }
      else { // count the history as well
        val pairs = streamMax.map(x => ("aggMax", x))
        val contMax = pairs.updateStateByKey[Double](updateMax)
        contMax.print()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
  
  def aggMin(arg: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Agg-Min")
    val ssc = new StreamingContext(sparkConf, Seconds(arg(3).toInt))
    
    val updateMin = (values: Seq[Double], state: Option[Double]) => {
     // val currMin = values.min
     var currMin = Double.MaxValue
     if (values.length > 0) {
       currMin = values.min
     }
      
      val preMin = state.getOrElse(Double.MaxValue)
      
      Option(math.min(currMin, preMin))
    }
    
    val queryRec = getPoints(arg(6))
    
    val src = ssc.socketTextStream(arg(1), arg(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    ssc.checkpoint("./spark-checkpoint/")
    
    // get the valid records first
    val test = src.map(_.split(" ")).filter(x => {
        if (x.length < 6) {
          false
        }
        else {
          true
        }
      })
      
    val pos = test.filter(x => isAllDigits(x(0))).map(x => (x(3).slice(0, 8).toDouble, x(4).slice(0, 8).toDouble))
    // TODO: need to further extract proper fields from records to proceed
    // currently using latitude value for testing purpose
    pos.print()  // print first 10 records of the latitude values
    
    val filterP1 = filterFunc(queryRec(0))(queryRec(1))_
   
    val queryMin= pos.filter(filterP1(_)).map(x => x._1)
    
    if (arg(4).toInt == 0) {
      println("Window size cannot be 0 for min()")
      System.exit(1)
    }
    else {
      val streamMin = queryMin.reduceByWindow(math.min, Seconds(arg(4).toInt), Seconds(arg(4).toInt))
      if (arg(5).toInt == 0) { // only count current window        
        streamMin.print()
      }
      else { // count the history as well
        val pairs = streamMin.map(x => ("aggMin", x))
        val contMin = pairs.updateStateByKey[Double](updateMin)
        contMin.print()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def aggSum(arg: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Agg-Sum")
    val ssc = new StreamingContext(sparkConf, Seconds(arg(3).toInt))
    
    val updateSum = (values: Seq[Double], state: Option[Double]) => {
      //val currSum = values.sum
      var currSum = 0.0
      
      if (values.length > 0) {
        currSum = values.sum
      }
      
      val preSum = state.getOrElse(0.0)
      
      Option(currSum + preSum)
    }
    
    val queryRec = getPoints(arg(6))
    
    val src = ssc.socketTextStream(arg(1), arg(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    ssc.checkpoint("./spark-checkpoint/")
    
    // get the valid records first
    val test = src.map(_.split(" ")).filter(x => {
        if (x.length < 6) {
          false
        }
        else {
          true
        }
      })
      
    val pos = test.filter(x => isAllDigits(x(0))).map(x => (x(3).slice(0, 8).toDouble, x(4).slice(0, 8).toDouble))
    // TODO: need to further extract proper fields from records to proceed
    // currently using latitude value for testing purpose
    pos.print()  // print first 10 records of the latitude values
    
    val filterP1 = filterFunc(queryRec(0))(queryRec(1))_
   
    val querySum= pos.filter(filterP1(_)).map(x => x._1)
    
    if (arg(4).toInt == 0) {
      println("Window size cannot be 0 for max()")
      System.exit(1)
    }
    else {
      val streamSum = querySum.reduceByWindow(_ + _, Seconds(arg(4).toInt), Seconds(arg(4).toInt))
      if (arg(5).toInt == 0) { // only count current window        
        streamSum.print()
      }
      else { // count the history as well
        val pairs = streamSum.map(x => ("aggSum", x))
        val contSum = pairs.updateStateByKey[Double](updateSum)
        contSum.print()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
  
    def aggAvg(arg: Array[String]) {
    // basic idea is to generate 2 Dstreams
    // one for the count, the other for the sum; then join the two stream together to get the final result
    val sparkConf = new SparkConf().setAppName("Agg-Average")
    val ssc = new StreamingContext(sparkConf, Seconds(arg(3).toInt))
    
    // update function for count
    val updateCnt = (values: Seq[Long], state: Option[Long]) => {
          val currCount = values.sum
      
          val preCount = state.getOrElse(0L)
       
          Option(currCount + preCount)
      }
    // update function for sum
    val updateSum = (values: Seq[Double], state: Option[Double]) => {
      //val currSum = values.sum
      var currSum = 0.0
      
      if (values.length > 0) {
        currSum = values.sum
      }
      
      val preSum = state.getOrElse(0.0)
      
      Option(currSum + preSum)
    }
    
    val queryRec = getPoints(arg(6))

    val src = ssc.socketTextStream(arg(1), arg(2).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    ssc.checkpoint("./spark-checkpoint/")
      
    // truncate the latitude and longitude to the first 8 digits
    val test = src.map(_.split(" ")).filter(x => {
        if (x.length < 6) {
          false
        }
        else {
          true
        }
      })
      
    val pos = test.filter(x => isAllDigits(x(0))).map(x => (x(3).slice(0, 8).toDouble, x(4).slice(0, 8).toDouble))
          
    pos.print()
          
    val filterP1 = filterFunc(queryRec(0))(queryRec(1))_
 
    // query contains all the qualified points
    val query = pos.filter(filterP1(_))
    val querySum = query.map(x => x._1)
    
    if (arg(4).toInt == 0) {
      println("Window size cannot be 0 for avg()")
      System.exit(1)
    }
    else {
      val streamSum = querySum.reduceByWindow(_ + _, Seconds(arg(4).toInt), Seconds(arg(4).toInt))
      val cnt = query.countByWindow(Seconds(arg(4).toInt), Seconds(arg(4).toInt))
      if (arg(5).toInt == 0) {
        val cntAvg = cnt.map(x => ("avg", x))        
        val sumAvg = streamSum.map(x => ("avg", x))
        // join two Dstreams together with key "avg"
        val joinAvg = cntAvg.join(sumAvg)
        val resAvg = joinAvg.map(x => {
          val curTuple = x._2;
          val curCnt = curTuple._1
          val curSum = curTuple._2
          if (curCnt != 0) {
            curSum / curCnt
          }
          else {
            0.0
          }
        })
        resAvg.map(x => ("windowAvg", x)).print()
      }
      else {
        val pairs = cnt.map(n => ("avgCont", n))
        val pairs2 = streamSum.map(x => ("aggCont", x))
        val cntAvg = pairs.updateStateByKey[Long](updateCnt)
        cntAvg.print()
        val contSum = pairs2.updateStateByKey[Double](updateSum)
        contSum.print()
        val joinAvg = cntAvg.join(contSum)
        joinAvg.print()
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  def getPoints(s: String): Array[(Double, Double)] = {
    val t = s.split(",")
    val p1 = (t(0).toDouble, t(1).toDouble)
    val p2 = (t(2).toDouble, t(3).toDouble)
    Array(p1, p2)
  }
  
  def isAllDigits(x: String): Boolean = {
    x forall Character.isDigit
  }
  
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.FATAL)
    }
  }
}
