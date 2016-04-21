package org.apache.spark.org.trustedanalytics.at.frame

import org.apache.spark.api.python.{ SerDeUtil => SparkSerDeUtil }

object PrivateSparkLib {
  val SerDeUtil = SparkSerDeUtil
  type AutoBatchedPickler = SerDeUtil.AutoBatchedPickler
}
