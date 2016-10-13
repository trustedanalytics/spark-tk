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
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameTransform }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait ReverseBoxCoxTransform extends BaseFrame {
  /**
   * Computes the reverse box-cox transformation for a column in the frame
   * @param columnName Name of the column to perform the reverse box-cox transformation on
   * @param lambdaValue Lambda power parameter
   * @param reverseBoxCoxColumnName Optional parameter specifying the name of column storing reverse box-cox computed value
   * @return A new column added to existing frame storing Reverse Box-cox value
   * Calculate the reverse box-cox transformation for each row in a frame using the given lambda value or default 0.
   *
   * The reverse box-cox transformation is computed by the following formula, where wt is a single entry box-cox value(row):
   *
   * yt = exp(wt); if lambda=0,
   * yt = (lambda * wt + 1)^(1/lambda) ; else
   *
   */
  def reverseBoxCox(columnName: String, lambdaValue: Double = 0d, reverseBoxCoxColumnName: Option[String] = None): Unit = {
    execute(ReverseBoxCox(columnName, lambdaValue, reverseBoxCoxColumnName))
  }
}

case class ReverseBoxCox(columnName: String, lambdaValue: Double, reverseBoxCoxColumnName: Option[String] = None) extends FrameTransform {

  require(columnName != null, "Column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val reverseBoxCoxRdd = ReverseBoxCox.reverseBoxCox(state, columnName, lambdaValue)

    // save results
    val updatedSchema = state.schema.addColumn(reverseBoxCoxColumnName.getOrElse(columnName + "_reverse_lambda_" + lambdaValue.toString), DataTypes.float64)

    FrameState(reverseBoxCoxRdd, updatedSchema)
  }
}

object ReverseBoxCox extends Serializable {
  /**
   * Computes the reverse Box-cox for a column in the frame
   * @param frameRdd Frame storing the data
   * @param columnName Name of the column to perform reverse Box-cox transform on
   * @param lambdaValue Lambda power parameter
   * @return Return a RDD[Row] with a reverse box-cox transform added to existing data
   */
  def reverseBoxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double): RDD[Row] = {
    frameRdd.mapRows(row => {
      val boxCox = row.doubleValue(columnName)
      val reverseBoxCox = computeReverseBoxCoxTransformation(boxCox, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ reverseBoxCox)
    })
  }

  /**
   * Compute the reverse box-cox transformation
   * @param boxCox The value whose reverse box-cox transformation is to be computed
   * @param lambdaValue Lambda power parameter
   * @return Reverse box-cox value
   */
  def computeReverseBoxCoxTransformation(boxCox: Double, lambdaValue: Double): Double = {
    val reverseBoxCox: Double = if (lambdaValue == 0d) math.exp(boxCox) else math.pow(lambdaValue * boxCox + 1, 1 / lambdaValue)
    reverseBoxCox
  }

}