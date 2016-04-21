package org.trustedanalytics.at.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.at.frame.{ Column, DataTypes }
import org.trustedanalytics.at.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait CumulativeSumTransform extends BaseFrame {

  def cumulativeSum(sampleCol: String): Unit = {
    execute(CumulativeSum(sampleCol))
  }
}

/**
 * Add column to frame with cumulative percent.
 *
 * @param sampleCol The name of the column from which to compute the cumulative percent.
 */
case class CumulativeSum(sampleCol: String) extends FrameTransform {
  require(StringUtils.isNotEmpty(sampleCol), "column name for sample is required")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativeSum(state, sampleCol)
    val updatedSchema = state.schema.addColumnFixName(Column(sampleCol + "_cumulative_sum", DataTypes.float64))

    // return result
    FrameState(cumulativeDistRdd, updatedSchema)
  }
}