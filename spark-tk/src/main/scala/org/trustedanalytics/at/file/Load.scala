package org.trustedanalytics.at.file

import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd

object Load {

  def loadParquet(path: String, sc: SparkContext): FrameRdd = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    FrameRdd.toFrameRdd(df)
  }

}
