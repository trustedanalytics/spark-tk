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

import org.joda.time.DateTime
import org.trustedanalytics.sparktk.frame.Frame
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait TimeSeriesSliceSummarization extends BaseFrame {
  /**
   * Returns a frame that is a sub-slice of the given series.
   *
   * Splits a time series frame on the specified start and end date/times.
   *
   * @param dateTimeIndex DateTimeIndex to conform all series to.
   * @param start The start date for the slice
   * @param end The end date for the slice (inclusive)
   * @return Frame that contain data between the specified start and end dates/times.
   */
  def timeSeriesSlice(dateTimeIndex: List[DateTime],
                      start: DateTime,
                      end: DateTime): Frame = {
    execute(TimeSeriesSlice(dateTimeIndex, start, end))
  }
}

case class TimeSeriesSlice(dateTimeIndex: List[DateTime],
                           start: DateTime,
                           end: DateTime) extends FrameSummarization[Frame] {

  require(dateTimeIndex != null && dateTimeIndex.nonEmpty, "date/time index is required")
  require(start != null, "start date is required")
  require(end != null, "end date is required")
  require(start.compareTo(end) < 0, "start date must be less than end date")

  override def work(state: FrameState): Frame = {
    val dates = TimeSeriesFunctions.createDateTimeIndex(dateTimeIndex)

    // Discover the column names of the key and value column
    val columnNames = TimeSeriesFunctions.discoverKeyAndValueColumns(state.schema)

    // Create TimeSeriesRDD
    val timeSeriesRdd = TimeSeriesFunctions.createTimeSeriesRDD(columnNames.keyColumnName, columnNames.valueColumnName, state, dates)

    // Perform Slice
    val sliced = timeSeriesRdd.slice(TimeSeriesFunctions.parseZonedDateTime(start), TimeSeriesFunctions.parseZonedDateTime(end))

    // Convert back to a frame to return
    TimeSeriesFunctions.createFrameFromTimeSeries(sliced, columnNames.keyColumnName, columnNames.valueColumnName)
  }

}

