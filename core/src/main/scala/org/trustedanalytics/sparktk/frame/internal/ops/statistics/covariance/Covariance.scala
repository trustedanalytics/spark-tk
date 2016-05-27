package org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CovarianceSummarization extends BaseFrame {
  /**
   * Calculate covariance for exactly two columns.
   *
   * @note This method applies only to columns containing numerical data.
   *
   * @param columnNameA The name of the column from which to compute the covariance.
   * @param columnNameB The name of the column from which to compute the covariance.
   * @return Covariance of the two columns.
   */
  def covariance(columnNameA: String,
                 columnNameB: String): Double = {
    execute(Covariance(columnNameA, columnNameB))
  }
}

case class Covariance(columnNameA: String,
                      columnNameB: String) extends FrameSummarization[Double] {
  lazy val dataColumnNames = List(columnNameA, columnNameB)
  require(dataColumnNames.forall(StringUtils.isNotEmpty(_)), "data column names cannot be null or empty.")

  override def work(state: FrameState): Double = {
    state.schema.validateColumnsExist(dataColumnNames)

    CovarianceFunctions.covariance(state, dataColumnNames)
  }

}

