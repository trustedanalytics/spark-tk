package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.spark.mllib.linalg.{ DenseVector, Vector => SparkVector, Matrix }

trait TimeSeriesBreuschGodfreyTestSummarization extends BaseFrame {
  /**
   * Calculates the Breusch-Godfrey test statistic for serial correlation.
   *
   * @param residuals Name of the column that contains residual (y) values
   * @param factors Name of the column(s) that contain factors (x) values
   * @param maxLag The lag order to calculate the test statistic.
   * @return The Breusch-Godfrey test statistic and p-value
   */
  def timeSeriesBreuschGodfreyTest(residuals: String,
                                   factors: Seq[String],
                                   maxLag: Int): BgTestReturn = {
    execute(TimeSeriesBreuschGodfreyTest(residuals, factors, maxLag))
  }
}

case class TimeSeriesBreuschGodfreyTest(residuals: String,
                                        factors: Seq[String],
                                        maxLag: Int) extends FrameSummarization[BgTestReturn] {
  require(StringUtils.isNotEmpty(residuals), "residuals name must not be null or empty.")
  require(factors != null && factors.nonEmpty, "factors string must not be null or empty.")

  override def work(state: FrameState): BgTestReturn = {
    val (vector, matrix) = TimeSeriesFunctions.getSparkVectorYAndXFromFrame(new FrameRdd(state.schema, state.rdd), residuals, factors)

    val result = TimeSeriesStatisticalTests.bgtest(vector, matrix, maxLag)
    return BgTestReturn(result._1, result._2)
  }
}

/**
 * Return value for the BreuschGodfreyTest
 * @param testStat Breusch-Godfrey test statistic
 * @param pValue p-value
 */
case class BgTestReturn(testStat: Double, pValue: Double)