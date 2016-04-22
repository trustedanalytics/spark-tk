package org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait TallyPercentTransform extends BaseFrame {

  def tallyPercent(sampleCol: String,
                   countVal: String): Unit = {
    execute(TallyPercent(sampleCol, countVal))
  }
}

/**
 * Compute a cumulative percent count.
 *
 * @param sampleCol The name of the column from which to compute the cumulative sum.
 * @param countVal The column value to be used for the counts.
 */
case class TallyPercent(sampleCol: String,
                        countVal: String) extends FrameTransform {
  require(StringUtils.isNotEmpty(sampleCol), "column name for sample is required")
  require(StringUtils.isNotEmpty(countVal), "count value for the sample is required")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativePercentCount(state, sampleCol, countVal)
    val updatedSchema = state.schema.addColumnFixName(Column(sampleCol + "_tally_percent", DataTypes.float64))
    FrameState(cumulativeDistRdd, updatedSchema)
  }
}