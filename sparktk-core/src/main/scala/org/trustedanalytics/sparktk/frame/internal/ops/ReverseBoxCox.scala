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

  def reverseBoxCox(columnName: String, lambdaValue: Double = 0d): Unit = {
    execute(ReverseBoxCox(columnName, lambdaValue))
  }
}

case class ReverseBoxCox(columnName: String, lambdaValue: Double) extends FrameTransform {

  require(columnName != null, "Column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val reverseBoxCoxRdd = ReverseBoxCox.reverseBoxCox(state, columnName, lambdaValue)

    // save results
    val updatedSchema = state.schema.addColumn(columnName + "_reverse_lambda_" + lambdaValue.toString, DataTypes.float64)

    FrameState(reverseBoxCoxRdd, updatedSchema)
  }
}

object ReverseBoxCox extends Serializable {
  /**
   * Computes the reverse boxcox transform for each row of the frame
   *
   */
  def reverseBoxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double): RDD[Row] = {
    frameRdd.mapRows(row => {
      val boxCox = row.doubleValue(columnName)
      val reverseBoxCox = computeReverseBoxCoxTransformation(boxCox, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ reverseBoxCox)
    })
  }

  def computeReverseBoxCoxTransformation(boxCox: Double, lambdaValue: Double): Double = {
    val reverseBoxCox: Double = if (lambdaValue == 0d) math.exp(boxCox) else math.pow(lambdaValue * boxCox + 1, 1 / lambdaValue)
    reverseBoxCox
  }

}