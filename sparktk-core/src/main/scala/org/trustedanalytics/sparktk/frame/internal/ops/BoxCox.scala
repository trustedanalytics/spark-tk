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

trait BoxCoxTransform extends BaseFrame {

  def boxCox(columnName: String, lambdaValue: Double = 0d, boxCoxColumnName: Option[String] = None): Unit = {
    execute(BoxCox(columnName, lambdaValue, boxCoxColumnName))
  }
}

case class BoxCox(columnName: String, lambdaValue: Double, boxCoxColumnName: Option[String] = None) extends FrameTransform {

  require(columnName != null, "Column name cannot be null")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val boxCoxRdd = BoxCox.boxCox(state, columnName, lambdaValue, boxCoxColumnName)

    // save results
    val updatedSchema = state.schema.addColumn(boxCoxColumnName.getOrElse(columnName + "_lambda_" + lambdaValue.toString), DataTypes.float64)

    FrameState(boxCoxRdd, updatedSchema)
  }
}

object BoxCox extends Serializable {
  /**
   * Computes the boxcox transform for each row of the frame
   *
   */
  def boxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double, boxCoxColumnName: Option[String] = None): RDD[Row] = {
    frameRdd.mapRows(row => {
      val y = row.doubleValue(columnName)
      val boxCox = computeBoxCoxTransformation(y, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ boxCox)
    })
  }

  def computeBoxCoxTransformation(y: Double, lambdaValue: Double): Double = {
    val boxCox: Double = if (lambdaValue == 0d) math.log(y) else (math.pow(y, lambdaValue) - 1) / lambdaValue
    boxCox
  }

}