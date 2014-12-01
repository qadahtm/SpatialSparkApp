package org.apache.spark.streaming

import org.apache.spark.streaming.dstream.DStream

object ExposeStreamingMethods {
 
}

case class DStreamWrapper[T](val dstream:DStream[T]) {
  def getOrCompute(time:Time) = dstream.getOrCompute(time)
}