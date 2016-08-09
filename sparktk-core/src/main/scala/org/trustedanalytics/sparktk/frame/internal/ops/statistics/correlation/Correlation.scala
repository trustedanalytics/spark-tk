package org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CorrelationSummarization extends BaseFrame {
  /**
   * Calculate correlation for two columns of the current frame.
   *
   * @note This method applies only to columns containing numerical data.
   *
   * @param columnNameA The name of the column to compute the correlation.
   * @param columnNameB The name of the column to compute the correlation.
   * @return Pearson correlation coefficient of the two columns.
   */
  def correlation(columnNameA: String,
                  columnNameB: String): Double = {
    execute(Correlation(columnNameA, columnNameB))
  }
}

case class Correlation(columnNameA: String, columnNameB: String) extends FrameSummarization[Double] {
  lazy val dataColumnNames = List(columnNameA, columnNameB)
  require(dataColumnNames.forall(StringUtils.isNotEmpty(_)), "data column names cannot be null or empty.")

  override def work(state: FrameState): Double = {
    state.schema.validateColumnsExist(dataColumnNames)

    // Calculate correlation
    CorrelationFunctions.correlation(state, dataColumnNames)
  }

}

