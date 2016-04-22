package org.trustedanalytics.sparktk.frame.internal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.Schema

case class FrameState(rdd: RDD[Row], schema: Schema)
