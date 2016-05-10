package org.trustedanalytics.sparktk.frame.internal.ops.flatten

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait FlattenColumnsTransform extends BaseFrame {

  def flattenColumns(columns: List[(String, Option[String])]): Unit = {
    execute(FlattenColumns(columns))
  }
}

/**
 * Spread data to multiple rows based on cell data.
 *
 * @param columns The columns to be flattened, with an optional delimiter.  The default delimiter is a comma (,).
 */
case class FlattenColumns(columns: List[(String, Option[String])]) extends FrameTransform {
  require(columns != null && columns.nonEmpty, "column list is required")
  columns.foreach {
    case (columnName, delimiter) => require(StringUtils.isNotBlank(columnName), "non empty column names are required.")
  }

  override def work(state: FrameState): FrameState = {
    var flattener: RDD[Row] => RDD[Row] = null
    val columnInfo = columns.map(c => (state.schema.columnIndex(c._1), state.schema.columnDataType(c._1), c._2.getOrElse(",")))

    var schema = state.schema

    for (column <- columnInfo) {
      column._2 match {
        case DataTypes.vector(length) =>
          schema = schema.convertType(column._1, DataTypes.float64)
        case DataTypes.string => // pass; no nothing
        case _ =>
          val illegalDataType = column._2.toString
          throw new IllegalArgumentException(s"Invalid column ('${schema.columnNames(column._1)}') data type provided: $illegalDataType. Only string or vector columns can be flattened.")
      }
    }

    flattener = FlattenColumnsFunctions.flattenRddByColumnIndices(columnInfo)

    // run the operation
    val flattenedRDD = flattener(state.rdd)

    // return result frame
    FrameState(flattenedRDD, schema)
  }
}