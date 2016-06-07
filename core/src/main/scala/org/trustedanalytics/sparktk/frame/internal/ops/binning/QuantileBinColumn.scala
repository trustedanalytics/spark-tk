package org.trustedanalytics.sparktk.frame.internal.ops.binning

import org.trustedanalytics.sparktk.frame.internal.{ FrameTransformReturn, FrameTransformWithResult, BaseFrame, FrameState }
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes }

trait QuantileBinColumnTransformWithResult extends BaseFrame {
  /**
   * Classify column into groups with the same frequency.
   *
   * Group rows of data based on the value in a single column and add a label to identify grouping.
   *
   * Equal depth binning attempts to label rows such that each bin contains the same number of elements.
   * For :math:`n` bins of a column :math:`C` of length :math:`m`, the bin number is determined by:
   *
   * .. math::
   *
   *    \lceil n * \frac { f(C) }{ m } \rceil
   *
   * where :math:`f` is a tie-adjusted ranking function over values of :math:`C`. If there are multiples of the
   * same value in :math:`C`, then their tie-adjusted rank is the average of their ordered rank values.
   *
   * @note
   *       1. Unicode in column names is not supported and will likely cause the drop_frames() method (and others)
   *       to fail!
   *       1. The num_bins parameter is considered to be the maximum permissible number of bins because the data may
   *       dictate fewer bins. For example, if the column to be binned has a quantity of :math"`X` elements with only
   *       2 distinct values and the *num_bins* parameter is greater than 2, then the actual number of bins will only
   *       be 2. This is due to a restriction that elements with an identical value must belong to the same bin.
   *
   * @param column The column whose values are to be binned.
   * @param numBins The maximum number of quantiles.  Default is the Square-root choice
   *                :math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.
   * @param binColumnName The name for the new column holding the grouping labels. Default is <column>_binned.
   */
  def quantileBinColumn(column: String,
                        numBins: Option[Int] = None,
                        binColumnName: Option[String] = None): Seq[Double] = {
    execute(QuantileBinColumn(column, numBins, binColumnName))
  }
}

case class QuantileBinColumn(column: String,
                             numBins: Option[Int],
                             binColumnName: Option[String]) extends FrameTransformWithResult[Seq[Double]] {

  override def work(state: FrameState): FrameTransformReturn[Seq[Double]] = {
    val columnIndex = state.schema.columnIndex(column)
    state.schema.requireColumnIsNumerical(column)
    val newColumnName = binColumnName.getOrElse(state.schema.getNewColumnName(s"${column}_binned"))
    val calculatedNumBins = HistogramFunctions.getNumBins(numBins, state.rdd)
    val binnedRdd = DiscretizationFunctions.binEqualDepth(columnIndex, calculatedNumBins, None, state.rdd)

    FrameTransformReturn(FrameState(binnedRdd.rdd, state.schema.copy(columns = state.schema.columns :+ Column(newColumnName, DataTypes.int32))), binnedRdd.cutoffs)
  }

}

