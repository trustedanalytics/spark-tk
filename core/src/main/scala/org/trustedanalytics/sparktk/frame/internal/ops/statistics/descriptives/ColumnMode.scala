package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ColumnModeSummarization extends BaseFrame {

  def columnMode(dataColumn: String,
                 weightsColumn: Option[String],
                 maxModesReturned: Option[Int]): ColumnModeReturn = {
    execute(ColumnMode(dataColumn, weightsColumn, maxModesReturned))
  }
}

/**
 * Calculate modes of a column.
 *
 * @param dataColumnName Name of the column supplying the data.
 * @param weightsColumn Name of the column supplying the weights.
 *                      Default is all items have weight of 1.
 * @param maxModesReturned Maximum number of modes returned. Default is 1.
 */
case class ColumnMode(dataColumnName: String,
                      weightsColumn: Option[String],
                      maxModesReturned: Option[Int]) extends FrameSummarization[ColumnModeReturn] {
  require(dataColumnName != null, "data column is required")

  override def work(state: FrameState): ColumnModeReturn = {
    // run the operation and return results
    val dataColumn = state.schema.column(dataColumnName)

    val weightsColumnIndexAndType: Option[(Int, DataType)] = weightsColumn match {
      case None =>
        None
      case Some(weightColumnName) =>
        Some((state.schema.columnIndex(weightsColumn.get), state.schema.columnDataType(weightsColumn.get)))
    }

    val modeCountOption = maxModesReturned

    ColumnStatistics.columnMode(state.schema.columnIndex(dataColumnName),
      dataColumn.dataType,
      weightsColumnIndexAndType,
      modeCountOption,
      state.rdd)
  }
}

/**
 *  * Mode data for a dataframe column.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 *
 * @param modes A mode is a data element of maximum net weight. A set of modes is returned.
 *             The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
 *             maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
 *             > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
 *             (per a canonical ordering) are returned.
 * @param weightOfMode Weight of the mode. (If no weight column is specified,
 *                     this is the number of appearances of the mode.)
 * @param totalWeight Total weight in the column. (If no weight column is specified, this is the number of entries
 *                    with finite, non-zero weight.)
 * @param modeCount The number of modes in the data.
 */
case class ColumnModeReturn(modes: Any, weightOfMode: Double, totalWeight: Double, modeCount: Long) {
}
