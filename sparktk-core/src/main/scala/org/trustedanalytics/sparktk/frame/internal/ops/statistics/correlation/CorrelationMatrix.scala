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

