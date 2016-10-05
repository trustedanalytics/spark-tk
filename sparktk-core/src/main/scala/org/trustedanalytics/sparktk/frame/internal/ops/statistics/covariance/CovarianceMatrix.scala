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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance

import org.trustedanalytics.sparktk.frame.DataTypes.vector
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.{ SchemaHelper, DataTypes, Frame }

trait CovarianceMatrixSummarization extends BaseFrame {
  /**
   * Calculate covariance matrix for two or more columns.
   *
   * @note This function applies only to columns containing numerical data.
   *
   * @param dataColumnNames The names of the columns from whic to compute the matrix.  Names should refer
   *                        to a single column of type vector, or two or more columns of numeric scalars.
   * @return A matrix with the covariance values for the columns.
   */
  def covarianceMatrix(dataColumnNames: List[String]): Frame = {

    execute(CovarianceMatrix(dataColumnNames))
  }
}

case class CovarianceMatrix(dataColumnNames: List[String]) extends FrameSummarization[Frame] {
  require(!dataColumnNames.contains(null), "data columns names cannot be null")
  require(dataColumnNames.forall(!_.equals("")), "data columns names cannot be empty")

  override def work(state: FrameState): Frame = {
    state.schema.requireColumnsAreVectorizable(dataColumnNames)

    // compute covariance
    val outputColumnDataType = state.schema.columnDataType(dataColumnNames.head)
    val outputVectorLength: Option[Long] = outputColumnDataType match {
      case vector(length) => Some(length)
      case _ => None
    }

    val covarianceRdd = CovarianceFunctions.covarianceMatrix(state, dataColumnNames, outputVectorLength)
    val outputSchema = SchemaHelper.create(dataColumnNames, DataTypes.float64, outputVectorLength)

    new Frame(covarianceRdd, outputSchema)
  }

}
