package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait DropColumnsTransform extends BaseFrame {
  /**
   * Drops columns from the frame
   *
   * The data from the columns is lost.
   *
   * @note It is not possible to delete all columns from a frame.  At least one column needs to remain. If it is
   *       necessary to delete all columns, then delete the frame.
   *
   * @param columns names of the columns to drop
   */
  def dropColumns(columns: Seq[String]): Unit = execute(DropColumns(columns))
}

case class DropColumns(columns: Seq[String]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    state.schema.validateColumnsExist(columns)
    require(state.schema.columnNamesExcept(columns).nonEmpty, "Cannot drop all columns, must leave at least one column")
    (state: FrameRdd).selectColumns(state.schema.dropColumns(columns).columnNames)
  }
}