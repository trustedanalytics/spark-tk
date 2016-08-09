package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait CategoricalSummarySummarization extends BaseFrame {
  /**
   * Compute a summary of the data in a column(s) for categorical or numerical data types.
   *
   * Optional parameters:
   *
   *  - top_k : ''int''<br>Displays levels which are in the top k most frequently occurring values for that column.
   * Default is 10.
   *  - threshold : ''float''<br>Displays levels which are above the threshold percentage with respect to the total
   * row count.  Default is 0.0.
   *
   * Compute a summary of the data in a column(s) for categorical or numerical data types. The returned value is an
   * Array containing categorical summary for each specified column.
   *
   * For each column, levels which satisfy the top k and/or threshold cutoffs are displayed along with their frequency
   * and percentage occurrence with respect to the total rows in the dataset.
   *
   * Performs level pruning first based on top k and then filters out levels which satisfy the threshold criterion.
   *
   * Missing data is reported when a column value is empty ("") or null.
   *
   * All remaining data is grouped together in the Other category and its frequency and percentage are reported as well.
   *
   * User must specify the column name and can optionally specify top_k and/or threshold.
   *
   * @param columns List of column names
   * @param topK Optional parameter for specifying to display levels which are in the top k most frequently
   *             occurring values for that column.  Default topK is 10.
   * @param threshold Optional parameter for specifying to display levels which are above the threshold
   *                  percentage with respect to the total row count.  If both topK and threshold are
   *                  specified, first performs level pruning based on top k, then filters out levels
   *                  which satisfy the threshold criterion.  Default threshold is 0.0.
   * @return Summary for specified column(s) consisting of levels with their frequency and percentage.
   */
  def categoricalSummary(columns: Seq[String],
                         topK: Option[Seq[Option[Int]]] = None,
                         threshold: Option[Seq[Option[Double]]] = None): Array[CategoricalSummaryOutput] = {
    require(columns.nonEmpty, "Column Input must not be empty. Please provide at least a single Column Input")
    val topKValues = topK.getOrElse(Seq.fill(columns.size) { None })
    val thresholdValues = threshold.getOrElse(Seq.fill(columns.size) { None })
    execute(CategoricalSummary(columns, topKValues, thresholdValues))
  }
}

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

