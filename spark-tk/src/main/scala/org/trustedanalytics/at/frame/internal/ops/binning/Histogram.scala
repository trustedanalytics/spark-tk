package org.trustedanalytics.at.frame.internal.ops.binning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

object HistogramFunctions {
  val MAX_COMPUTED_NUMBER_OF_BINS: Int = 1000
  val UNWEIGHTED_OBSERVATION_SIZE: Double = 1.0

  def getNumBins(numBins: Option[Int], rdd: RDD[Row]): Int = {
    numBins match {
      case Some(n) => n
      case None =>
        math.min(math.floor(math.sqrt(rdd.count)), HistogramFunctions.MAX_COMPUTED_NUMBER_OF_BINS).toInt
    }
  }
}
