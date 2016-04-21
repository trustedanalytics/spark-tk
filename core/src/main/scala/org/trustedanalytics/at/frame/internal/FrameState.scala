package org.trustedanalytics.at.frame.internal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.Schema

case class FrameState(rdd: RDD[Row], schema: Schema)
