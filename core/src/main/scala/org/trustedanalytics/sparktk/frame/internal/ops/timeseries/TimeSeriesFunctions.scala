/**
 *  Copyright (c) 2015 Intel Corporation 
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

package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import java.io.Serializable
import java.sql.Timestamp
import java.time.ZonedDateTime
import org.apache.spark.mllib.linalg.{ Vector, DenseVector }
import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema, Schema, Frame }
import com.cloudera.sparkts._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

/**
 * Object contains utility functions for working with time series
 */
object TimeSeriesFunctions extends Serializable {

  // Spark SQL UDF for converting our datetime column (represented as a long) to a Timestamp datatype
  val toTimestamp: UserDefinedFunction = udf((t: Long) => new Timestamp(t))

  /**
   * Creates a Frame for the specified TimeSeriesRdd
   *
   * @param timeseriesRdd TimeSeriesRDD
   * @param keyColumn Name of the key column
   * @param valueColumn Name of column that contains a series of values for each key
   * @return FrameRdd
   */
  def createFrameFromTimeSeries(timeseriesRdd: TimeSeriesRDD[String], keyColumn: String, valueColumn: String): Frame = {
    // Create frame schema
    val timeseriesSchema = FrameSchema(Vector(Column(keyColumn, DataTypes.string), Column(valueColumn, DataTypes.vector(timeseriesRdd.index.size))))

    // Map the column of values so that it uses a Scala Vector rather than a Spark DenseVector.
    val withVector = timeseriesRdd.map(row => {
      val originalColumns = row.productIterator.toList
      val newVectorCol = originalColumns(1).asInstanceOf[DenseVector].toArray.iterator.toVector
      Array[Any](originalColumns(0), newVectorCol)
    })

    // Create FrameRdd to return
    new Frame(withVector, timeseriesSchema)
  }

  /**
   * Creates a DateTimeIndex from the ist of Date/Times
   * @param dateTimeList List of Date/Times
   * @return DateTimeIndex
   */
  def createDateTimeIndex(dateTimeList: List[DateTime]): DateTimeIndex = {
    implicit val ordering = new Ordering[DateTime] {
      override def compare(a: DateTime, b: DateTime): Int = {
        a.compareTo(b)
      }
    }

    // Create DateTimeIndex after parsing the strings as ZonedDateTime
    DateTimeIndex.irregular(dateTimeList.sorted.map(dt => parseZonedDateTime(dt)).toArray)
  }

  /**
   * Parses the DateTime as a ZonedDateTime
   * @param dateTime Date/time
   * @return ZonedDateTime
   */
  def parseZonedDateTime(dateTime: DateTime): ZonedDateTime = {
    ZonedDateTime.parse(dateTime.toString(ISODateTimeFormat.dateTime))
  }

  /**
   * Return value to discoverKeyAndValueColumns
   * @param keyColumnName Name of the key column
   * @param valueColumnName Name of the value column
   */
  case class KeyAndValueColumnReturn(keyColumnName: String, valueColumnName: String)

  /**
   * Discovers the names of the column that contain the string key and vector of time series values.  The schema
   * provided is expected to be for a time series frame, where we just have 2 columns: (1) String column that contains
   * the key, and (2) Vector column tha contains the time series values.  If these exact columns are not found,
   * exceptions are thrown.
   * @param schema Schema for a time series frame
   * @return Name of the key and value columns in the KeyAndValueColumnReturn object
   */
  def discoverKeyAndValueColumns(schema: Schema): KeyAndValueColumnReturn = {
    if (schema.columns.size != 2)
      throw new RuntimeException("Frame has unsupported number of columns.  Time series frames are only expected to have 2 columns -- a string column (key) and a vector column (series values).")

    val first = schema.columns.head
    val second = schema.columns.last

    // Look for the names of the string column (key) and vector column (value) to return.
    (first.dataType, second.dataType) match {
      case (DataTypes.string, DataTypes.vector(length)) => KeyAndValueColumnReturn(first.name, second.name)
      case (DataTypes.vector(length), DataTypes.string) => KeyAndValueColumnReturn(second.name, first.name)
      case _ => throw new RuntimeException(s"Frame has unsupported column datatypes.  Expected a string and vector column, but found ${first.dataType} and ${second.dataType}.")
    }
  }

  /**
   * Creates a TimeSeriesRDD from the specified SparkFrame, with the DateTimeIndex provided
   * @param keyColumn Name of the key colum
   * @param valueColumn Name of the value column
   * @param frame Frame to use to create the TimeSeries RDD.  This frame should already be formatted
   *              as a time series.
   * @param dateTimeIndex DateTime index for the time series
   * @return TimeSeriesRDD
   */
  def createTimeSeriesRDD(keyColumn: String, valueColumn: String, frame: FrameRdd, dateTimeIndex: DateTimeIndex): TimeSeriesRDD[String] = {
    if (dateTimeIndex == null)
      throw new IllegalArgumentException("DateTimeIndex is required for creating a TimeSeriesRDD.")

    // Create TimeSeriesRDD
    val rdd = frame.mapRows(row => {
      val key = row.stringValue(keyColumn)
      val series = row.vectorValue(valueColumn)
      val vector = new DenseVector(series.toArray)
      (key.asInstanceOf[String], vector.asInstanceOf[Vector])
    })

    new TimeSeriesRDD[String](dateTimeIndex, rdd)
  }

}
