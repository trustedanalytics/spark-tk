package org.trustedanalytics.at.frame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.schema.Schema

case class FrameState(rdd: RDD[Row], schema: Schema)
