package org.apache.spark.mllib.org.trustedanalytics.sparktk

import org.apache.spark.mllib.util.{ Loader => SparkLoader, NumericParser => SparkNumericParser }
import org.apache.spark.mllib.linalg.{ Vectors => SparkVectors, Vector => SparkVector }

object MllibAliases {

  val Loader = SparkLoader
  val NumericParser = SparkNumericParser
  def parseNumeric(any: Any): SparkVector = SparkVectors.parseNumeric(any)

}

