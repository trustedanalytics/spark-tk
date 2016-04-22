package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait DropColumnsTransform extends BaseFrame {

  def dropColumns(columns: Seq[String]): Unit = execute(DropColumns(columns))
}

/**
 * Drops columns from the frame
 *
 * @param columns names of the columns to drop
 */
case class DropColumns(columns: Seq[String]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    state.schema.validateColumnsExist(columns)
    require(state.schema.columnNamesExcept(columns).nonEmpty, "Cannot drop all columns, must leave at least one column")
    (state: FrameRdd).selectColumns(state.schema.dropColumns(columns).columnNames)
  }
}