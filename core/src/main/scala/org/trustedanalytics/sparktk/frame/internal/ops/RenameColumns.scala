package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait RenameColumnsTransform extends BaseFrame {
  def renameColumns(names: Map[String, String]): Unit = {
    execute(RenameColumns(names))
  }
}

/**
 * Rename columns
 *
 * @param names Map of old names to new names.
 */
case class RenameColumns(names: Map[String, String]) extends FrameTransform {
  require(names != null, "names parameter is required.")

  override def work(state: FrameState): FrameState = {
    var frame = state
    names foreach {
      case (oldName, newName) =>
        frame = (state: FrameRdd).renameColumn(oldName, newName)
    }
    frame
  }
}