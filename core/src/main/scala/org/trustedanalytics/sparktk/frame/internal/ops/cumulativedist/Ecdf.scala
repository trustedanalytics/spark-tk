package org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.{ Frame, FrameSchema, Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait EcdfSummarization extends BaseFrame {

  def ecdf(column: String): Frame = {
    execute(Ecdf(column))
  }
}

/**
 * Builds a new frame with columns for data and distribution.
 *
 * @param column The name of the input column containing sample
 */
case class Ecdf(column: String) extends FrameSummarization[Frame] {
  require(StringUtils.isNotEmpty(column), "column is required")

  override def work(state: FrameState): Frame = {
    val sampleColumn = state.schema.column(column)
    require(sampleColumn.dataType.isNumerical, s"Invalid column ${sampleColumn.name} for ECDF.  Expected a numeric data type, but got ${sampleColumn.dataType}.")
    val ecdfSchema = FrameSchema(Vector(sampleColumn.copy(), Column(sampleColumn.name + "_ecdf", DataTypes.float64)))

    // Create new frame with the result
    new Frame(CumulativeDistFunctions.ecdf(state, sampleColumn), ecdfSchema)
  }
}

