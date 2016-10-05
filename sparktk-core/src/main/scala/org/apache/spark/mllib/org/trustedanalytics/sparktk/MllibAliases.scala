/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.spark.mllib.org.trustedanalytics.sparktk

import org.apache.spark.mllib.linalg.{ Vector => SparkVector, Vectors => SparkVectors, MatrixUDT => SparkMatrixUDT }
import org.apache.spark.mllib.util.{ Loader => SparkLoader, NumericParser => SparkNumericParser }

object MllibAliases {

  val Loader = SparkLoader
  type MllibVector = SparkVector // public, but makes it easier to get at here
  val MllibVectors = SparkVectors // public, but makes it easier to get at here
  val NumericParser = SparkNumericParser
  def parseNumeric(any: Any): SparkVector = SparkVectors.parseNumeric(any)
  type MatrixUDT = SparkMatrixUDT
}

