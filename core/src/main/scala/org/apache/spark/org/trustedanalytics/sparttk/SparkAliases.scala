package org.apache.spark.org.trustedanalytics.sparktk

import org.apache.spark.api.python.{ SerDeUtil => SparkSerDeUtil }
import org.apache.spark.util.{ BoundedPriorityQueue => SparkBoundedPriorityQueue }
import org.apache.spark.util.collection.{ Utils => SparkCollectionUtils }

object SparkAliases {

  val SerDeUtil = SparkSerDeUtil
  type AutoBatchedPickler = SerDeUtil.AutoBatchedPickler
  type BoundedPriorityQueue[A] = SparkBoundedPriorityQueue[A]
  val CollectionUtils = SparkCollectionUtils

}
