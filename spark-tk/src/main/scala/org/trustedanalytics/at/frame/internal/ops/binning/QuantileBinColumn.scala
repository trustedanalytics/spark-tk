package org.trustedanalytics.at.frame.internal.ops.binning

import org.trustedanalytics.at.frame.internal.{ FrameTransformReturn, FrameTransformWithResult, BaseFrame, FrameState }
import org.trustedanalytics.at.frame.{ Column, DataTypes }

trait QuantileBinColumnTransformWithResult extends BaseFrame {
  def quantileBinColumn(column: String,
                        numBins: Option[Int] = None,
                        binColumnName: Option[String] = None): Array[Double] = {
    execute(QuantileBinColumn(column, numBins, binColumnName))
  }
}

/**
 * Classify column into groups with the same frequency.
 * @param column The column whose values are to be binned.
 * @param numBins The maximum number of quantiles.  Default is the Square-root choice
 *                :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
 * @param binColumnName The name for the new column holding the grouping labels. Default is <column>_binned.
 */
case class QuantileBinColumn(column: String,
                             numBins: Option[Int],
                             binColumnName: Option[String]) extends FrameTransformWithResult[Array[Double]] {

  override def work(state: FrameState): FrameTransformReturn[Array[Double]] = {
    val columnIndex = state.schema.columnIndex(column)
    state.schema.requireColumnIsNumerical(column)
    val newColumnName = binColumnName.getOrElse(state.schema.getNewColumnName(s"${column}_binned"))
    val calculatedNumBins = HistogramFunctions.getNumBins(numBins, state.rdd)
    val binnedRdd = DiscretizationFunctions.binEqualDepth(columnIndex, calculatedNumBins, None, state.rdd)

    FrameTransformReturn(FrameState(binnedRdd.rdd, state.schema.copy(columns = state.schema.columns :+ Column(newColumnName, DataTypes.int32))), binnedRdd.cutoffs)
  }

}

