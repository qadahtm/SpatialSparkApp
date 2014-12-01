package org.apache.spark.sql.stream

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.rdd.RDD


case class MutableExistingRdd(output: Seq[Attribute], var rdd: RDD[Row]) 
extends org.apache.spark.sql.execution.LeafNode {
  override def execute() = rdd
}