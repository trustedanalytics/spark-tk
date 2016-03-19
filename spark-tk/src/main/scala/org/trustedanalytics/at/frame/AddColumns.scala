package org.trustedanalytics.at.frame

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.trustedanalytics.at.interfaces.{ RowWrapper, BaseFrame, FrameTransform, ImmutableFrame }
import org.trustedanalytics.at.schema.FrameSchema

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

  override def work(immutableFrame: ImmutableFrame): ImmutableFrame = {
    //val predictionsRDD = frameRdd.mapRows(row => {
    //val columnsArray = row.valuesAsDenseVector(kmeansColumns).toArray
    val frameRdd = new FrameRdd(immutableFrame.schema, immutableFrame.rdd)
    val addedRdd = frameRdd.mapRows(row => Row.merge(row.data, rowFunction(row).data))
    ImmutableFrame(addedRdd, immutableFrame.schema.copy(columns = immutableFrame.schema.columns ++ schema.columns))
  }
}