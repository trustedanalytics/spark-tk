package org.trustedanalytics.at.frame.ops.binning

import org.trustedanalytics.at.frame.schema.Column
import org.trustedanalytics.at.frame.{ BaseFrame, FrameState, FrameTransform }

trait BinColumnTrait extends BaseFrame {

  def binColumn(column: String,
                cutoffs: List[Double],
                includeLowest: Boolean = true,
                strictBinning: Boolean = false,
                binColumnName: Option[String] = None): Unit = {

    execute(BinColumn(column, cutoffs, includeLowest, strictBinning, binColumnName))
  }
}

/**
 *
 * @param column the column to bin
 * @param cutoffs list of bin cutoff points, which must be progressively increasing; all bin boundaries must be
 *                defined, so with N bins, N+1 values are required
 * @param includeLowest true means the lower bound is inclusive, where false means the upper bound is inclusive
 * @param strictBinning if true, each value less than the first cutoff value or greater than the last cutoff value
 *                      will be assigned to a bin value of -1; if false, values less than the first cutoff value will
 *                      be placed in the first bin, and those beyond the last cutoff will go in the last bin
 * @param binColumnName The name of the new column may be optionally specified
 */
case class BinColumn(column: String, //column: Column,
                     cutoffs: List[Double],
                     includeLowest: Boolean,
                     strictBinning: Boolean,
                     binColumnName: Option[String]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    val columnIndex = state.schema.columnIndex(column)
    state.schema.requireColumnIsNumerical(column)
    val newColumnName = binColumnName.getOrElse(state.schema.getNewColumnName(column + "_binned"))
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, cutoffs, includeLowest, strictBinning, state.rdd)
    FrameState(binnedRdd, state.schema.copy(columns = state.schema.columns :+ Column(newColumnName, "int32")))
  }

}

