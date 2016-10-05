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
package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ DataTypes, Frame }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import com.cloudera.sparkts._

trait TimeSeriesFromObseravationsSummarization extends BaseFrame {
  /**
   * Returns a frame that has the observations formatted as a time series.
   *
   * Uses the specified timestamp, key, and value columns and the date/time index provided to format the observations
   * as a time series.  The time series frame will have columns for the key and a vector of the observed values that
   * correspond to the date/time index.
   *
   * @param dateTimeIndex DateTimeIndex to conform all series to.
   * @param timestampColumn The name of the column telling when the observation occurred.
   * @param keyColumn The name of the column that contains which string key the observation belongs to.
   * @param valueColumn The name of the column that contains the observed value.
   * @return Frame formatted as a time series.
   */
  def timeSeriesFromObseravations(dateTimeIndex: List[DateTime],
                                  timestampColumn: String,
                                  keyColumn: String,
                                  valueColumn: String): Frame = {
    execute(TimeSeriesFromObseravations(dateTimeIndex, timestampColumn, keyColumn, valueColumn))
  }
}

case class TimeSeriesFromObseravations(dateTimeIndex: List[DateTime],
                                       timestampColumn: String,
                                       keyColumn: String,
                                       valueColumn: String) extends FrameSummarization[Frame] {
  require(dateTimeIndex != null && dateTimeIndex.nonEmpty, "date/time index is required")
  require(StringUtils.isNotEmpty(timestampColumn), "timestamp column is required")
  require(StringUtils.isNotEmpty(keyColumn), "key column is required")
  require(StringUtils.isNotEmpty(valueColumn), "value column is required")
  override def work(state: FrameState): Frame = {
    // Column validation
    state.schema.validateColumnsExist(List(timestampColumn, keyColumn, valueColumn))
    state.schema.requireColumnIsType(timestampColumn, DataTypes.datetime)
    state.schema.requireColumnIsType(valueColumn, DataTypes.float64)

    // Get DateTimeIndex
    val dates = TimeSeriesFunctions.createDateTimeIndex(dateTimeIndex)

    // Create DataFrame with a new column that's formatted as a Timestamp (because this is what timeSeriesRDDFromObservations requires)
    val newTimestampColumn = timestampColumn + "_as_timestamp" // name for the new timestamp formatted column
    val dataFrame = (state: FrameRdd).toDataFrame
    val dataFrameWithTimestamp = dataFrame.withColumn(newTimestampColumn, TimeSeriesFunctions.toTimestamp(dataFrame(timestampColumn)))

    // Convert the frame of observations to a TimeSeriesRDD
    val timeseriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dates, dataFrameWithTimestamp, newTimestampColumn, keyColumn, valueColumn)

    // Convert back to a frame to return
    TimeSeriesFunctions.createFrameFromTimeSeries(timeseriesRdd, keyColumn, valueColumn)
  }

}

