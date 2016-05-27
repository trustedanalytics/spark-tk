package org.trustedanalytics.sparktk.frame.internal.ops.statistics.quantiles

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.{ Column, FrameSchema, DataTypes, Frame }

trait QuantilesSummarization extends BaseFrame {
  /**
   * Calculate quantiles on the given column.
   *
   * @param column the name of the column to calculate quantiles.
   * @param quantiles What is being requested
   * @return A new frame with two columns (''float64''): requested quantiles and their respective values.
   */
  def quantiles(column: String,
                quantiles: List[Double]): Frame = {

    execute(Quantiles(column, quantiles))
  }
}

case class Quantiles(column: String,
                     quantiles: List[Double]) extends FrameSummarization[Frame] {

  override def work(state: FrameState): Frame = {
    val columnIndex = state.schema.columnIndex(column)

    // New schema for the quantiles frame
    val schema = FrameSchema(Vector(Column("Quantiles", DataTypes.float64), Column(column + "_QuantileValue", DataTypes.float64)))

    // return frame with quantile values
    new Frame(QuantilesFunctions.quantiles(state.rdd, quantiles, columnIndex, state.rdd.count()), schema)
  }
}

/**
 * Quantile composing element which contains element's index and its weight
 * @param index element index
 * @param quantileTarget the quantile target that the element can be applied to
 */
case class QuantileComposingElement(index: Long, quantileTarget: QuantileTarget)

