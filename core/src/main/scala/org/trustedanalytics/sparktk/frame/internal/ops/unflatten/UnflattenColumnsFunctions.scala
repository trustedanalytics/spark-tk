package org.trustedanalytics.sparktk.frame.internal.ops.unflatten

import org.trustedanalytics.sparktk.frame.{ Schema, DataTypes, Column }
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Functions used for the UnflattenColumns operation.
 */
object UnflattenColumnsFunctions extends Serializable {

  def createTargetSchema(schema: Schema, compositeKeyNames: List[String]): Schema = {
    val keys = schema.copySubset(compositeKeyNames)
    val converted = schema.columnsExcept(compositeKeyNames).map(col => Column(col.name, DataTypes.string))

    keys.addColumns(converted)
  }

  def unflattenRddByCompositeKey(compositeKeyIndex: List[Int],
                                 initialRdd: RDD[(Vector[Any], Iterable[Row])],
                                 targetSchema: Schema,
                                 delimiter: String): RDD[Row] = {
    val rowWrapper = new RowWrapper(targetSchema)
    val unflattenRdd = initialRdd.map { case (key, row) => key.toArray ++ unflattenRowsForKey(compositeKeyIndex, row, delimiter) }

    unflattenRdd.map(row => rowWrapper.create(row))
  }

  private def unflattenRowsForKey(compositeKeyIndex: List[Int], groupedByRows: Iterable[Row], delimiter: String): Array[Any] = {

    val rows = groupedByRows.toList
    val rowCount = rows.length

    val colsInRow = rows.head.length
    val result = new Array[Any](colsInRow)

    //all but the last line + with delimiter
    for (i <- 0 to rowCount - 2) {
      val row = rows(i)
      addRowToResults(row, compositeKeyIndex, result, delimiter)
    }

    //last line, no delimiter
    val lastRow = rows(rowCount - 1)
    addRowToResults(lastRow, compositeKeyIndex, result, StringUtils.EMPTY)

    result.filter(_ != null)
  }

  private def addRowToResults(row: Row, compositeKeyIndex: List[Int], results: Array[Any], delimiter: String): Unit = {
    for (j <- 0 until row.length) {
      if (!compositeKeyIndex.contains(j)) {
        val value = row.apply(j) + delimiter
        if (results(j) == null) {
          results(j) = value
        }
        else {
          results(j) += value
        }
      }
    }
  }
}
