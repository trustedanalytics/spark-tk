package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait DropDuplicatesTransform extends BaseFrame {
  /**
   * Modify the current frame, removing duplicate rows.
   *
   * Remove data rows which are the same as other rows. The entire row can be checked for duplication, or the search
   * for duplicates can be limited to one or more columns.  This modifies the current frame.
   *
   * @param uniqueColumns Column name(s) to identify duplicates. Default is the entire row is compared.
   */
  def dropDuplicates(uniqueColumns: Option[Seq[String]]): Unit = {
    execute(DropDuplicates(uniqueColumns))
  }
}

case class DropDuplicates(uniqueColumns: Option[Seq[String]]) extends FrameTransform {
  override def work(state: FrameState): FrameState = {
    uniqueColumns match {
      case Some(columns) =>
        val columnNames = state.schema.validateColumnsExist(columns).toVector
        (state: FrameRdd).dropDuplicatesByColumn(columnNames)
      case None =>
        // If no specific columns are specified, return distinct rows across all columns
        FrameState(state.rdd.distinct(), state.schema)
    }
  }
}