package org.trustedanalytics.at.frame.internal.ops

import org.apache.spark.SparkContext
import org.trustedanalytics.at.frame.internal.rdd.FrameRdd

object Load {

  def loadParquet(path: String, sc: SparkContext): FrameRdd = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    FrameRdd.toFrameRdd(df)
  }

}
