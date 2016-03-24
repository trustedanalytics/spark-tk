package org.trustedanalytics.at.frame.ops

import org.apache.spark.org.trustedanalytics.at.frame.FrameRdd
import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.{ RowWrapper, FrameState, FrameTransform, BaseFrame }
import org.trustedanalytics.at.frame.schema.FrameSchema

trait AddColumnsTrait extends BaseFrame {

  def addColumns(rowFunction: RowWrapper => RowWrapper,
                 schema: FrameSchema): Unit = {
    execute(AddColumns(rowFunction, schema))
  }
}

/**
 * Adds columns to frame according to row function (UDF)
 *
 * @param rowFunction map function which produces new row columns
 * @param schema schema of the new columns being added
 */
case class AddColumns(rowFunction: RowWrapper => RowWrapper,
                      schema: FrameSchema) extends FrameTransform {

  override def work(state: FrameState): FrameState = {
    //val predictionsRDD = frameRdd.mapRows(row => {
    //val columnsArray = row.valuesAsDenseVector(kmeansColumns).toArray
    val frameRdd = new FrameRdd(state.schema, state.rdd)
    val addedRdd = frameRdd.mapRows(row => Row.merge(row.data, rowFunction(row).data))
    FrameState(addedRdd, state.schema.copy(columns = state.schema.columns ++ schema.columns))
  }
}