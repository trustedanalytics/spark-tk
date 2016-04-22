package org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CorrelationSummarization extends BaseFrame {

  def correlation(columnNameA: String,
                  columnNameB: String): Double = {
    execute(Correlation(columnNameA, columnNameB))
  }
}

/**
 * Calculate correlation for two columns of the current frame.
 *
 * @param columnNameA The name of the column to compute the correlation.
 * @param columnNameB The name of the column to compute the correlation.
 */
case class Correlation(columnNameA: String, columnNameB: String) extends FrameSummarization[Double] {
  lazy val dataColumnNames = List(columnNameA, columnNameB)
  require(dataColumnNames.forall(StringUtils.isNotEmpty(_)), "data column names cannot be null or empty.")

  override def work(state: FrameState): Double = {
    state.schema.validateColumnsExist(dataColumnNames)

    // Calculate correlation
    CorrelationFunctions.correlation(state, dataColumnNames)
  }

}

