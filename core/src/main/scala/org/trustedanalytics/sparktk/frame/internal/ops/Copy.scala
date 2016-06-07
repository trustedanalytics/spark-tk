package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.Frame

trait CopySummarization extends BaseFrame {
  /**
   * Copies specified columns into a new Frame object, optionally renaming them and/or filtering them.
   *
   * @param columns Optional dictionary of column names to include in the copy and target names.  The default
   *                behavior is that all columns will be included in the frame that is returned.
   * @param where Optional function to filter the rows that are included.  The default behavior is that
   *              all rows will be included in the frame that is returned.
   * @return New frame object.
   */
  def copy(columns: Option[Map[String, String]] = None,
           where: Option[Row => Boolean] = None): Frame = {
    execute(Copy(columns, where))
  }
}

case class Copy(columns: Option[Map[String, String]] = None,
                where: Option[Row => Boolean]) extends FrameSummarization[Frame] {
  override def work(state: FrameState): Frame = {

    val finalSchema = columns.isDefined match {
      case true => state.schema.copySubsetWithRename(columns.get)
      case false => state.schema
    }

    var filteredRdd = if (where.isDefined) state.rdd.filter(where.get) else state.rdd

    if (columns.isDefined) {
      val frameRdd = new FrameRdd(state.schema, filteredRdd)
      filteredRdd = frameRdd.selectColumnsWithRename(columns.get)
    }

    new Frame(filteredRdd, finalSchema)
  }
}

