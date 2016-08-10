package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

trait TimeSeriesDurbinWatsonTestSummarization extends BaseFrame {
  /**
   * Computes the Durbin-Watson test statistic used to determine the presence of serial correlation in the residuals.
   * Serial correlation can show a relationship between values separated from each other by a given time lag. A value
   * close to 0.0 gives evidence for positive serial correlation, a value close to 4.0 gives evidence for negative
   * serial correlation, and a value close to 2.0 gives evidence for no serial correlation.
   *
   * @param residuals Name of the column that contains the residual values
   * @return The Durbin-Watson test statistic
   */
  def timeSeriesDurbinWatsonTest(residuals: String): Double = {
    execute(TimeSeriesDurbinWatsonTest(residuals))
  }
}

case class TimeSeriesDurbinWatsonTest(residuals: String) extends FrameSummarization[Double] {
  require(StringUtils.isNotEmpty(residuals), "residuals must not be null or empty.")

  override def work(state: FrameState): Double = {
    TimeSeriesStatisticalTests.dwtest(TimeSeriesFunctions.getVectorFromFrame(state, residuals))
  }
}

