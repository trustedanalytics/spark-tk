package org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait CumulativePercentTransform extends BaseFrame {
  /**
   * Add column to frame with cumulative percent.
   *
   * A cumulative percent sum is computed by sequentially stepping through the rows, observing the column values and
   * keeping track of the current percentage of the total sum accounted for at the current value.
   *
   * @note This method applies only to columns containing numerical data. Although this method will execute for
   *       columns containing negative values, the interpretation of the result will change (for example, negative
   *       percentages).
   *
   * @param sampleCol The name of the column from which to compute the cumulative percent.
   */
  def cumulativePercent(sampleCol: String): Unit = {
    execute(CumulativePercent(sampleCol))
  }
}

case class CumulativePercent(sampleCol: String) extends FrameTransform {
  require(StringUtils.isNotEmpty(sampleCol), "column name for sample is required")
  override def work(state: FrameState): FrameState = {
    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativePercentSum(state, sampleCol)
    val updatedSchema = state.schema.addColumnFixName(Column(sampleCol + "_cumulative_percent", DataTypes.float64))

    // return result
    FrameState(cumulativeDistRdd, updatedSchema)
  }
}