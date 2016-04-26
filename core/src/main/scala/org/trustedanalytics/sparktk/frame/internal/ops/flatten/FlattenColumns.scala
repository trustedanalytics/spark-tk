package org.trustedanalytics.sparktk.frame.internal.ops.flatten

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait FlattenColumnsTransform extends BaseFrame {

  def flattenColumns(columns: List[String],
                     delimiters: Option[List[String]]): Unit = {
    execute(FlattenColumns(columns, delimiters))
  }
}

/**
 * Spread data to multiple rows based on cell data.
 *
 * @param columns The columns to be flattened.
 * @param delimiters The list of delimiter strings for each column.  Default is comma (,).
 */
case class FlattenColumns(columns: List[String],
                          delimiters: Option[List[String]]) extends FrameTransform {
  require(columns != null && columns.nonEmpty, "column list is required")
  columns.foreach(x => require(StringUtils.isNotBlank(x), "non empty column names are required."))

  // If just one delimiter is defined, use the same delimiter for all columns.  Otherwise, use the default.
  lazy val defaultDelimiters = Array.fill(columns.size) { if (delimiters.isDefined && delimiters.get.size == 1) delimiters.get(0) else "," }

  override def work(state: FrameState): FrameState = {
    var flattener: RDD[Row] => RDD[Row] = null
    val columnIndices = columns.map(c => state.schema.columnIndex(c))
    val columnDataTypes = columns.map(c => state.schema.columnDataType(c))
    var schema = state.schema

    var stringDelimiterCount = 0 // counter used to track delimiters for string columns

    for (i <- columns.indices) {
      columnDataTypes(i) match {
        case DataTypes.vector(length) =>
          schema = state.schema.convertType(columns(i), DataTypes.float64)
        case DataTypes.string =>
          if (delimiters.isDefined && delimiters.get.size > 1) {
            if (delimiters.get.size > stringDelimiterCount) {
              defaultDelimiters(i) = delimiters.get(stringDelimiterCount)
              stringDelimiterCount += 1
            }
            else
              throw new IllegalArgumentException(s"The number of delimiters provided is less than the number of string columns being flattened.")
          }
        case _ =>
          val illegalDataType = columnDataTypes(i).toString
          throw new IllegalArgumentException(s"Invalid column ('${columns(i)}') data type provided: $illegalDataType. Only string or vector columns can be flattened.")
      }
    }

    if (stringDelimiterCount > 0 && stringDelimiterCount < delimiters.get.size)
      throw new IllegalArgumentException(s"The number of delimiters provided is more than the number of string columns being flattened.")

    flattener = FlattenColumnsFunctions.flattenRddByColumnIndices(columnIndices, columnDataTypes, defaultDelimiters.toList)

    // run the operation
    val flattenedRDD = flattener(state.rdd)

    // return result frame
    FrameState(flattenedRDD, schema)
  }
}