package org.apache.spark.org.trustedanalytics.sparktk

import org.apache.spark.api.python.{ SerDeUtil => SparkSerDeUtil, PythonUtils => SparkPythonUtils }
import org.apache.spark.util.{ BoundedPriorityQueue => SparkBoundedPriorityQueue }
import org.apache.spark.util.collection.{ Utils => SparkCollectionUtils }
import org.apache.spark.api.java.{ JavaUtils => SparkJavaUtils }

import org.apache.spark.mllib.api.python.{ SerDe => SparkMLLibSerDe }

object SparkAliases {

  val SerDeUtil = SparkSerDeUtil

  lazy val MLLibSerDe = getSparkSerDeUtil

  def getSparkSerDeUtil = {
    SparkMLLibSerDe
    SparkMLLibSerDe.initialize()
  }

  type AutoBatchedPickler = SerDeUtil.AutoBatchedPickler
  type BoundedPriorityQueue[A] = SparkBoundedPriorityQueue[A]
  val CollectionUtils = SparkCollectionUtils
  val JavaUtils = SparkJavaUtils
  val PythonUtils = SparkPythonUtils
}
