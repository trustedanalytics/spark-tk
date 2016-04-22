package org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance

import org.trustedanalytics.sparktk.frame.DataTypes.vector
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.{ SchemaHelper, DataTypes, Frame }

trait CovarianceMatrixSummarization extends BaseFrame {

  def covarianceMatrix(dataColumNames: List[String]): Frame = {

    execute(CovarianceMatrix(dataColumNames))
  }
}

/**
 * Calculate covariance matrix for two or more columns.
 *
 * @param dataColumnNames The names of the columns from whic to compute the matrix.  Names should refer
 *                        to a single column of type vector, or two or more columns of numeric scalars.
 */
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
