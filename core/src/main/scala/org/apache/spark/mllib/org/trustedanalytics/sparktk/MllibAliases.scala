package org.apache.spark.mllib.org.trustedanalytics.sparktk

import org.apache.spark.mllib.util.{ Loader => SparkLoader, NumericParser => SparkNumericParser }
import org.apache.spark.mllib.linalg.{ Vectors => SparkVectors, Vector => SparkVector }

object MllibAliases {

  val Loader = SparkLoader
  type MllibVector = SparkVector // public, but makes it easier to get at here
  val MllibVectors = SparkVectors // public, but makes it easier to get at here
  val NumericParser = SparkNumericParser
  def parseNumeric(any: Any): SparkVector = SparkVectors.parseNumeric(any)
}

