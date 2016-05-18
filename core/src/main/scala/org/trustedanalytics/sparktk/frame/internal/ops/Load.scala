package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.SparkContext
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

object Load {

  def loadParquet(path: String, sc: SparkContext): FrameRdd = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet(path)
    FrameRdd.toFrameRdd(df)
  }

  def loadFromCsv(path: String,
                  delimiter: String = ",",
                  header: Boolean = false,
                  inferSchema: Boolean = false,
                  sc: SparkContext): FrameRdd = {
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    val headerStr = header.toString.toLowerCase
    val inferSchemaStr = inferSchema.toString.toLowerCase

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", headerStr)
      .option("inferSchema", inferSchemaStr)
      .option("delimiter", delimiter)
      .load(path)
    FrameRdd.toFrameRdd(df)
  }

}
