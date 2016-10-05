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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.{ SchemaHelper, DataTypes, Frame }

trait CorrelationMatrixSummarization extends BaseFrame {
  /**
   * Calculate correlation matrix for two or more columns.
   *
   * @note This method applies only to columns containing numerical data.
   *
   * @param dataColumnNames The names of the columns from which to compute the matrix.
   * @return A Frame with the matrix of the correlation values for the columns.
   */
  def correlationMatrix(dataColumnNames: List[String]): Frame = {

    execute(CorrelationMatrix(dataColumnNames))
  }
}

case class CorrelationMatrix(dataColumnNames: List[String]) extends FrameSummarization[Frame] {
  require(dataColumnNames.size >= 2, "two or more data columns are required")
  require(!dataColumnNames.contains(null), "data columns names cannot be null")
  require(dataColumnNames.forall(!_.equals("")), "data columns names cannot be empty")

  override def work(state: FrameState): Frame = {
    state.schema.validateColumnsExist(dataColumnNames)

    val correlationRdd = CorrelationFunctions.correlationMatrix(state, dataColumnNames)
    val outputSchema = SchemaHelper.create(dataColumnNames, DataTypes.float64)

    new Frame(correlationRdd, outputSchema)
  }

}

