package org.trustedanalytics.at.frame.internal.ops

import org.apache.spark.org.trustedanalytics.at.frame.FrameRdd
import org.trustedanalytics.at.frame.internal._

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