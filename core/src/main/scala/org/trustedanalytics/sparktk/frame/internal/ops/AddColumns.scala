package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait AddColumnsTransform extends BaseFrame {

  def addColumns(rowFunction: RowWrapper => Row,
                 newColumns: Seq[Column]): Unit = {
    execute(AddColumns(rowFunction, newColumns))
  }

}

/**
 * Adds columns to frame according to row function (UDF)
 *
 * @param rowFunction map function which produces new row columns
 * @param newColumns sequence of the new columns being added (Schema)
 */
case class AddColumns(rowFunction: RowWrapper => Row,
                      newColumns: Seq[Column]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    val frameRdd = new FrameRdd(state.schema, state.rdd)
    val addedRdd = frameRdd.mapRows(row => Row.merge(row.data, rowFunction(row)))
    FrameState(addedRdd, state.schema.copy(columns = state.schema.columns ++ newColumns))
  }
}