package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ColumnMedianSummarization extends BaseFrame {

  def columnMedian(dataColumn: String, weightsColumn: Option[String]): ColumnMedianReturn = {
    execute(ColumnMedian(dataColumn, weightsColumn))
  }
}

/**
 * Calculate the (weighted) median of a column.
 *
 * @param dataColumn The column whose median is to be calculated.
 * @param weightsColumn The column that provides weights (frequencies) for the median calculation.
 *                      Must contain numerical data.
 *                      Default is all items have a weight of 1.
 */
case class ColumnMedian(dataColumn: String, weightsColumn: Option[String]) extends FrameSummarization[ColumnMedianReturn] {
  require(dataColumn != null, "data column is required")

  override def work(state: FrameState): ColumnMedianReturn = {
    val columnIndex = state.schema.columnIndex(dataColumn)
    val valueDataType = state.schema.columnDataType(dataColumn)

    // run the operation and return results
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = state.schema.columnIndex(weightsColumn.get)
      (Some(weightsColumnIndex), Some(state.schema.columnDataType(weightsColumn.get)))
    }
    ColumnStatistics.columnMedian(columnIndex, valueDataType, weightsColumnIndexOption, weightsDataTypeOption, state.rdd)
  }
}

/**
 * The median value of the (possibly weighted) column. None when the sum of the weights is 0.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 *
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 * @param value The median. None if the net weight of the column is 0.
 */
case class ColumnMedianReturn(value: Any)
