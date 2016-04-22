package org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait TallyTransform extends BaseFrame {
  def tally(sampleCol: String,
            countVal: String): Unit = {
    execute(Tally(sampleCol, countVal))
  }
}

/**
 * Count number of times a value is seen.
 *
 * @param sampleCol The name of the column from which to compute the cumulative count.
 * @param countVal The column value to be used for the counts.
 */
case class Tally(sampleCol: String,
                 countVal: String) extends FrameTransform {
  require(StringUtils.isNotEmpty(sampleCol), "column name for sample is required")
  require(StringUtils.isNotEmpty(countVal), "count value for the sample is required")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativeCount(state, sampleCol, countVal)
    val updatedSchema = state.schema.addColumnFixName(Column(sampleCol + "_tally", DataTypes.float64))
    FrameState(cumulativeDistRdd, updatedSchema)
  }
}