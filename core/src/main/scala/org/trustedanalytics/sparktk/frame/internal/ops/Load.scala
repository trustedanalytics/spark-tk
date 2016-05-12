package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.SparkContext
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

object Load {

  /**
   * Loads a parquet file found at the given path and returns a Frame
   * @param sc active SparkContext
   * @param path path to the file
   * @param tkFormatVersion TK metadata formatVersion
   * @param tkMetadata TK metadata
   * @return
   */
  def loadFrameParquet(sc: SparkContext, path: String, tkFormatVersion: Int = 1, tkMetadata: JValue = null): Frame = {
    require(tkFormatVersion == 1, s"Frame load only supports version 1.  Got version $tkFormatVersion")
    // no extra metadata in version 1
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    val frameRdd = FrameRdd.toFrameRdd(df)
    new Frame(frameRdd, frameRdd.frameSchema)
  }

}
