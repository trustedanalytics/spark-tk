/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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
                                 initialRdd: RDD[(Seq[Any], Iterable[Row])],
                                 targetSchema: Schema,
                                 delimiter: String): RDD[Row] = {
    val rowWrapper = new RowWrapper(targetSchema)
    val unflattenRdd = initialRdd.map { case (key, row) => key.toArray ++ unflattenRowsForKey(compositeKeyIndex, row, delimiter) }

    unflattenRdd.map(row => rowWrapper.create(row))
  }

  private def unflattenRowsForKey(compositeKeyIndices: List[Int], groupedByRows: Iterable[Row], delimiter: String): Array[Any] = {
    val colsInRow = groupedByRows.head.length
    val result = new Array[Any](colsInRow)

    val rowIndices = (0 until colsInRow).toList.filter((i: Int) => !compositeKeyIndices.contains(i))
    val rowIter = groupedByRows.iterator

    // first row
    if (rowIter.hasNext)
      addFirstRowToResults(rowIter.next(), rowIndices, result, if (rowIter.hasNext) delimiter else StringUtils.EMPTY)

    // add on the rest of the rows
    while (rowIter.hasNext) {
      val row = rowIter.next()
      // Add row, but note that we only include the delimiter, if there's another row after this one.
      addRowToResults(row, rowIndices, result, if (rowIter.hasNext) delimiter else StringUtils.EMPTY)
    }

    result.filter(_ != null)
  }

  /**
   * Sets the specified row values in the results array.  This is used for the first
   * @param row
   * @param rowIndices
   * @param results
   * @param delimiter
   */
  private def addFirstRowToResults(row: Row, rowIndices: List[Int], results: Array[Any], delimiter: String): Unit = {
    for (index <- rowIndices) {
      results(index) = row.apply(index) + delimiter
    }
  }

  /**
   * Appends the specified row values to the results array
   * @param row Row values
   * @param indices Indices into the row
   * @param results Results array
   * @param delimiter Delimiter to use when appending to the reuslts array
   */
  private def addRowToResults(row: Row, indices: List[Int], results: Array[Any], delimiter: String): Unit = {
    for (index <- indices) {
      results(index) += row.apply(index) + delimiter
    }
  }
}
