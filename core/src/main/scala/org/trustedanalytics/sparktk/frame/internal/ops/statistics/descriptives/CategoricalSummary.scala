package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait CategoricalSummarySummarization extends BaseFrame {

  def categoricalSummary(columnInput: List[CategoricalColumnInput]): Array[CategoricalSummaryOutput] = {
    execute(CategoricalSummary(columnInput))
  }
}

/**
 * Compute a summary of the data in a column(s) for categorical or numerical data types.
 *
 * @param columnInput List of Categorical Column Input consisting of column, topk and/or threshold
 */
case class CategoricalSummary(columnInput: List[CategoricalColumnInput]) extends FrameSummarization[Array[CategoricalSummaryOutput]] {
  private val defaultTopK = 10
  private val defaultThreshold = 0.0

  require(columnInput.nonEmpty, "Column Input must not be empty. Please provide at least a single Column Input")

  override def work(state: FrameState): Array[CategoricalSummaryOutput] = {
    // Select each column and invoke summary statistics
    val selectedRdds: List[(FrameRdd, CategoricalColumnInput)] =
      columnInput.map(elem => ((state: FrameRdd).selectColumn(elem.column), elem))

    (for { rdd <- selectedRdds }
      yield CategoricalSummaryFunctions.getSummaryStatistics(
      rdd._1,
      state.rdd.count.asInstanceOf[Double],
      rdd._2.topK,
      rdd._2.threshold,
      defaultTopK,
      defaultThreshold)).toArray
  }
}

/**
 * Class used to specify inputs to categorical summary for each column.  The user must specify the column name and can
 * optionally specify the top_k and/or threshold.  The default is toe display all levels which are in the Top 10.
 *
 * @param column column name
 * @param topK Displays levels which are in the top k more frequently occurring values for that column
 * @param threshold Displays levels which are above the threshold percentage with respect to the total row count.
 */
case class CategoricalColumnInput(column: String, topK: Option[Int], threshold: Option[Double]) {
  require(!column.isEmpty && column != null, "Column name should not be empty or null")
  require(topK.isEmpty || topK.get > 0, "top_k input value should be greater than 0")
  require(threshold.isEmpty || (threshold.get >= 0.0 && threshold.get <= 1.0), "threshold should be greater than or equal to 0.0 and less than or equal to 1.0")
}

case class LevelData(level: String, frequency: Int, percentage: Double)

case class CategoricalSummaryOutput(column: String, levels: Array[LevelData])

