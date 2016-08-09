package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait AddColumnsTransform extends BaseFrame {
  /**
   * Adds columns to frame according to row function (UDF)
   *
   * Assigns data to column based on evaluating a function for each row.
   *
   * @note
   *       1. The rowFunction must return a value in the same format as specified by the schema.
   *
   * @param rowFunction map function which produces new row columns
   * @param newColumns sequence of the new columns being added (Schema)
   */
  def addColumns(rowFunction: RowWrapper => Row,
                 newColumns: Seq[Column]): Unit = {
    execute(AddColumns(rowFunction, newColumns))
  }

}

case class AddColumns(rowFunction: RowWrapper => Row,
                      newColumns: Seq[Column]) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    SchemaHelper.isMergeable(state.schema, new FrameSchema(newColumns))
    val frameRdd = new FrameRdd(state.schema, state.rdd)
    val addedRdd = frameRdd.mapRows(row => Row.merge(row.data, rowFunction(row)))
    FrameState(addedRdd, state.schema.copy(columns = state.schema.columns ++ newColumns))
  }
}