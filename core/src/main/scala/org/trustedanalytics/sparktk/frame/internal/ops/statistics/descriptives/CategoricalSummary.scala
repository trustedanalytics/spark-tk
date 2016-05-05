package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait CategoricalSummarySummarization extends BaseFrame {
  def categoricalSummary(columns: Seq[String],
                         topK: Option[Seq[Option[Int]]] = None,
                         threshold: Option[Seq[Option[Double]]] = None): Array[CategoricalSummaryOutput] = {
    require(columns.nonEmpty, "Column Input must not be empty. Please provide at least a single Column Input")
    val topKValues = topK.getOrElse(Seq.fill(columns.size) { None })
    val thresholdValues = threshold.getOrElse(Seq.fill(columns.size) { None })
    execute(CategoricalSummary(columns, topKValues, thresholdValues))
  }
}

/**
 * Compute a summary of the data in a column(s) for categorical or numerical data types.
 *
 * @param columns List of column names
 * @param topK Optional parameter for specifying to display levels which are in the top k most frequently
 *             occurring values for that column.  Default topK is 10.
 * @param threshold Optional parameter for specifying to display levels which are above the threshold
 *                  percentage with respect to the total row count.  If both topK and threshold are
 *                  specified, first performs level pruning based on top k, then filters out levels
 *                  which satisfy the threshold criterion.  Default threshold is 0.0.
 */
case class CategoricalSummary(columns: Seq[String],
                              topK: Seq[Option[Int]],
                              threshold: Seq[Option[Double]]) extends FrameSummarization[Array[CategoricalSummaryOutput]] {
  private val defaultTopK = 10
  private val defaultThreshold = 0.0

  require(topK.size == columns.size,
    s"The number of top k values (${topK.size}) must match the number of column names provided (${columns.size}).")
  require(threshold.size == columns.size,
    s"The number of threshold values (${threshold.size}) must match the number of column names provided (${columns.size}).")

  override def work(state: FrameState): Array[CategoricalSummaryOutput] = {
    (for ((columnName, topKValue, thresholdValue) <- (columns, topK, threshold).zipped)
      yield CategoricalSummaryFunctions.getSummaryStatistics(
      (state: FrameRdd).selectColumn(columnName),
      state.rdd.count.asInstanceOf[Double],
      topKValue,
      thresholdValue,
      defaultTopK,
      defaultThreshold)).toArray
  }
}

case class LevelData(level: String, frequency: Int, percentage: Double)

case class CategoricalSummaryOutput(column: String, levels: Array[LevelData])

