package org.trustedanalytics.at.frame.internal.ops.binning

import org.trustedanalytics.at.frame.internal.{ FrameTransformReturn, FrameTransformWithResult, BaseFrame, FrameState }
import org.trustedanalytics.at.frame.Column

trait BinColumnEqualTransformWithResult extends BaseFrame {
  def binColumnEqual(column: String,
                     numBins: Option[Int] = None,
                     binType: Option[String] = None,
                     binColumnName: Option[String] = None): Array[Double] = {
    execute(BinColumnEqual(column, numBins, binType, binColumnName))
  }
}

case class BinColumnEqual(column: String,
                          numBins: Option[Int],
                          binType: Option[String],
                          binColumnName: Option[String]) extends FrameTransformWithResult[Array[Double]] {

  override def work(state: FrameState): FrameTransformReturn[Array[Double]] = {
    val columnIndex = state.schema.columnIndex(column)
    state.schema.requireColumnIsNumerical(column)
    val newColumnName = binColumnName.getOrElse(state.schema.getNewColumnName(s"${column}_binned"))
    val calculatedNumBins = HistogramFunctions.getNumBins(numBins, state.rdd)
    val binnedRdd = binType.getOrElse("equalwidth").toLowerCase match {
      case "equaldepth" => DiscretizationFunctions.binEqualDepth(columnIndex, calculatedNumBins, None, state.rdd)
      case "equalwidth" => DiscretizationFunctions.binEqualWidth(columnIndex, calculatedNumBins, state.rdd)
      case default => throw new IllegalArgumentException(s"Unrecognized binning method: ${default}")
    }

    FrameTransformReturn(FrameState(binnedRdd.rdd, state.schema.copy(columns = state.schema.columns :+ Column(newColumnName, "int32"))), binnedRdd.cutoffs)
  }

}

