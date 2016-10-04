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
package org.trustedanalytics.sparktk.frame.internal.ops.flatten

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import java.util.regex.Pattern

/**
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object FlattenColumnsFunctions extends Serializable {

  /**
   * Flatten RDD by the column with specified column indices
   * @param columns List of tuples that contain column index, data type, and delimiters.
   * @param rdd RDD for flattening
   * @return new RDD with columns flattened
   */
  def flattenRddByColumnIndices(columns: List[(Int, DataType, String)])(rdd: RDD[Row]): RDD[Row] = {
    val flattener = flattenRowByColumnIndices(columns)_
    rdd.flatMap(row => flattener(row))
  }

  /**
   * flatten a row by the column with specified column indices.  Columns must be a string or vector.
   * @param columns List of tuples that contain column index, data type, and delimiter
   * @param row row data
   * @return flattened out row/rows
   */
  private[frame] def flattenRowByColumnIndices(columns: List[(Int, DataType, String)])(row: Row): Array[Row] = {
    val rowBuffer = new scala.collection.mutable.ArrayBuffer[Row]()
    for (i <- columns.indices) {
      val (columnIndex, columnDataType, delimiter) = columns(i)

      columnDataType match {
        case DataTypes.string =>
          val splitItems = row(columnIndex).asInstanceOf[String].split(Pattern.quote(delimiter))

          if (splitItems.length > 1) {
            // Loop through items being split from the string
            for (rowIndex <- splitItems.indices) {
              val isNewRow = rowBuffer.length <= rowIndex
              val r = if (isNewRow) row.toSeq.toArray.clone() else rowBuffer(rowIndex).toSeq.toArray.clone()

              r(columnIndex) = splitItems(rowIndex)

              if (isNewRow) {
                for (tempColIndex <- columns.indices) {
                  if (tempColIndex != i) {
                    r(columns(tempColIndex)._1) = null
                  }
                }

                rowBuffer += Row.fromSeq(r)
              }
              else
                rowBuffer(rowIndex) = Row.fromSeq(r)
            }
          }
          else {
            // There's nothing to split, just update first row in the rowBuffer
            if (rowBuffer.length == 0)
              rowBuffer += row
            else {
              val r = rowBuffer(0).toSeq.toArray.clone()
              r(columnIndex) = splitItems(0)
              rowBuffer(0) = Row.fromSeq(r)
            }
          }
        case DataTypes.vector(length) =>
          val vectorItems = DataTypes.toVector(length)(row(columnIndex)).toArray
          // Loop through items in the vector
          for (vectorIndex <- vectorItems.indices) {
            val isNewRow = rowBuffer.length <= vectorIndex
            val r = if (isNewRow) row.toSeq.toArray.clone() else rowBuffer(vectorIndex).toSeq.toArray.clone()
            // Set vector item in the column being flattened
            r(columnIndex) = vectorItems(vectorIndex)
            if (isNewRow) {
              // Empty out other columns that are being flattened in the new row
              for (tempColIndex <- columns.indices) {
                if (tempColIndex != i) {
                  r(columns(tempColIndex)._1) = null
                }
              }
              // Add new row to the rowBuffer
              rowBuffer += Row.fromSeq(r)
            }
            else
              rowBuffer(vectorIndex) = Row.fromSeq(r)
          }
        case _ =>
          throw new IllegalArgumentException("Flatten column does not support type: " + columnDataType.toString)
      }

    }

    rowBuffer.toArray
  }

}
