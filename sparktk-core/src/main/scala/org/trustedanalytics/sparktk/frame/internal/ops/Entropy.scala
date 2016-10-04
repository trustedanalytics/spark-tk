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
package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.NumericValidationUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives.ColumnStatistics
import scala.util.Try

trait EntropySummarization extends BaseFrame {
  /**
   * Calculate the Shannon entropy of a column.
   *
   * The data column is weighted via the weights column. All data elements of weight <= 0 are excluded from the
   * calculation, as are all data elements whose weight is NaN or infinite.  If there are no data elements with a
   * finite weight greater than 0, the entropy is zero.
   *
   * @param dataColumn The column whose entropy is to be calculated.
   * @param weightsColumn The column that provides weights (frequencies) for the entropy calculation.
   *                      Must contain numerical data.
   *                      Default is using uniform weights of 1 for all items.
   * @return Entropy.
   */
  def entropy(dataColumn: String, weightsColumn: Option[String] = None): Double = {
    execute(Entropy(dataColumn, weightsColumn))
  }
}

case class Entropy(dataColumn: String, weightsColumn: Option[String]) extends FrameSummarization[Double] {
  require(StringUtils.isNotEmpty(dataColumn), "data column name is required")

  override def work(state: FrameState): Double = {
    val columnIndex = state.schema.columnIndex(dataColumn)

    // Get the index and data type of the weights column, if a weights column was specified.
    val weightsColumnOption: Option[(Int, DataType)] = weightsColumn match {
      case None => None
      case Some(column) => Some((state.schema.columnIndex(column), state.schema.columnDataType(column)))
    }

    // run the operation and return results
    EntropyRddFunctions.shannonEntropy(state.rdd, columnIndex, weightsColumnOption)
  }
}

/**
 * Functions for computing entropy.
 *
 * Entropy is a measure of the uncertainty in a random variable.
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private object EntropyRddFunctions extends Serializable {

  /**
   * Calculate the Shannon entropy for specified column in data frame.
   *
   * @param frameRdd RDD for data frame
   * @param dataColumnIndex Index of data column
   * @param weightsColumnOption Option for column providing the weights (tuple with column index and data type)
   * @return Weighted shannon entropy (using natural log)
   */
  def shannonEntropy(frameRdd: RDD[Row],
                     dataColumnIndex: Int,
                     weightsColumnOption: Option[(Int, DataType)] = None): Double = {
    require(dataColumnIndex >= 0, "column index must be greater than or equal to zero")

    val dataWeightPairs =
      ColumnStatistics.getDataWeightPairs(dataColumnIndex, weightsColumnOption, frameRdd)
        .filter({ case (data, weight) => NumericValidationUtils.isFinitePositive(weight) })

    val distinctCountRDD = dataWeightPairs.reduceByKey(_ + _).map({ case (value, count) => count })

    // sum() throws an exception if RDD is empty so catching it and returning zero
    val totalCount = Try(distinctCountRDD.sum()).getOrElse(0d)

    if (totalCount > 0) {
      val distinctProbabilities = distinctCountRDD.map(count => count / totalCount)
      -distinctProbabilities.map(probability => if (probability > 0) probability * math.log(probability) else 0).sum()
    }
    else 0d
  }
}

