package utils

import spray.json.JsObject
import scala.collection.mutable.ArrayBuffer
import org.joda.time.DateTime
import spray.json.JsNumber
import spray.json.JsArray
import spray.json.JsString

case class QueryPoint(qid:Long,qpoint: Point2D) extends Serializable

case class Tweet(val createdAt:String, val text: String, val spatialPrams:SObject) {
  def id = spatialPrams.id
  
  def getJSONString() : String = {
    JsObject("id" -> JsNumber(spatialPrams.id), 
    		 "createdAt" -> JsString(createdAt),
    		 "lat" -> JsNumber(spatialPrams.y),
    		 "lng" -> JsNumber(spatialPrams.x),
    		 "text" -> JsString(text)).toString
  }
  
  override def toString() : String ={
    Array(spatialPrams.id.toString,createdAt,spatialPrams.y.toString,spatialPrams.x.toString,text).mkString(",")
  }
}

case class Point2D(x:Double,y:Double) extends Serializable {
  
  def toJSONString() :String = {
    val latlng = BerlinMODLatLngConverter.getLatLng(x, y)
    
    "{ \"lat\": "+latlng._1+", \"lng\" :"+latlng._2+"}"
  }
  override def toString() = {
    val latlng = BerlinMODLatLngConverter.getLatLng(x, y)    
    latlng._1+":"+latlng._2
  }
}



case class SObject(val id: Long, val x: Double, val y: Double) {
  
  def getLatLng() = {
    BerlinMODLatLngConverter.getLatLng(x,y)
  }

  override def toString() = {
    "{ \"oid\" : " + id + " , \"x\":" + x + ", \"y\":" + y + "}"
  }
  

  override def equals(o: Any) = o match {
    case that: SObject => that.id.equals(this.id)
    case _ => false
  }

}

case class MObject(val t: DateTime, sobj: SObject) {
  
  def getId = sobj.id
  
  override def toString() = {
    "{ \"timestamp\"" + t + ", \"sobj\" : " + sobj.toString + "}"
  }

  override def equals(o: Any) = o match {
    case that: MObject => that.sobj.id.equals(this.sobj.id) && that.t.equals(this.t)
    case _ => false
  }
}

trait Query extends Serializable

class QueryProcessingContext() {
  val _queries = ArrayBuffer[Query]()

  def addQuery(q: Query) = {
    _queries += q
  }
}

case class ResultMessage[T](val content:T)

case class RangeQuery(val qid: String, val points: Array[Point2D], val ts: Long) extends Query {

  val insidePoints = ArrayBuffer[SObject]()
  override def toString() = {
    "{\"QueryID\":" + qid + " }"
  }
  
  def toJSONString():String ={
    JsObject("qid" -> JsNumber(qid),
        "insidePoints" -> JsArray(insidePoints.map(x => JsString(x.toString)).toVector), 
        "qpoints" -> JsArray(points.map(x => JsString(x.toJSONString)).toVector))
        .toString
  }

  def alreadyInside(p: SObject): Boolean = { // linear search , can be improved. 
    val in = insidePoints.filter(_ == p)
    (in.size > 0)
  }

  def isInsideRange(p: SObject): Boolean = {
    var res = alreadyInside(p)
    if (!res) res = _contains(p)
    res
  }
  
  override def equals(o: Any) = o match {
    case that: RangeQuery => that.qid.equals(this.qid)
    case _ => false
  }

  private def _contains(mo: SObject): Boolean = {
    if (points.size == 2) // Box
    {
      val northeast = points(0)         
      val southwest = points(1) 
      
      val p = mo
      val res = (p.x <= northeast.x && p.x >= southwest.x && p.y >= southwest.y && p.y <= northeast.y)
      if (res) insidePoints += mo
      res
    } else {
      // multi point region
      false
    }
  }
}
