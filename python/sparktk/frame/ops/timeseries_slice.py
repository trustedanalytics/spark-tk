# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


def timeseries_slice(self, date_time_index, start, end):
    """
    Returns a frame split on the specified start and end date/times.

    Splits a time series frame on the specified start and end date/times.

    :param date_time_index: List of date/time strings. DateTimeIndex to conform all series to.
    :param start: The start date for the slice in the ISO 8601 format, like: yyyy-MM-dd'T'HH:mm:ss.SSSZ
    :param end: The end date for the slice in the ISO 8601 format, like: yyyy-MM-dd'T'HH:mm:ss.SSSZ
    :return: Frame that contains a sub-slice of the current frame, based on the specified start/end date/times.

    Examples
    --------
    For this example, we start with a frame that has already been formatted as a time series.
    This means that the frame has a string column for key and a vector column that contains
    a series of the observed values.  We must also know the date/time index that corresponds
    to the time series.

    The time series is in a Frame object called *ts_frame*.

    <hide>
    >>> from sparktk import dtypes
    >>> schema= [("key", str), ("series", dtypes.vector(6))]
    >>> data = [["A", [62,55,60,61,60,59]],["B", [60,58,61,62,60,61]],["C", [69,68,68,70,71,69]]]

    >>> ts_frame = tc.frame.create(data, schema)
    -etc-

    </hide>

        >>> ts_frame.inspect()
        [#]  key  series
        ==================================
        [0]  A    [62, 55, 60, 61, 60, 59]
        [1]  B    [60, 58, 61, 62, 60, 61]
        [2]  C    [69, 68, 68, 70, 71, 69]

    Next, we define the date/time index.  In this example, it is one day intervals from
    2016-01-01 to 2016-01-06:

        >>> datetimeindex = ["2016-01-01T12:00:00.000Z","2016-01-02T12:00:00.000Z","2016-01-03T12:00:00.000Z","2016-01-04T12:00:00.000Z","2016-01-05T12:00:00.000Z","2016-01-06T12:00:00.000Z"]

    Get a slice of our time series from 2016-01-02 to 2016-01-04:

        >>> slice_start = "2016-01-02T12:00:00.000Z"
        >>> slice_end = "2016-01-04T12:00:00.000Z"

        >>> sliced_frame = ts_frame.timeseries_slice(datetimeindex, slice_start, slice_end)
        <progress>

    Take a look at our sliced time series:

        >>> sliced_frame.inspect()
        [#]  key  series
        ============================
        [0]  A    [55.0, 60.0, 61.0]
        [1]  B    [58.0, 61.0, 62.0]
        [2]  C    [68.0, 68.0, 70.0]

    """
    if not isinstance(date_time_index, list):
        raise TypeError("date_time_index should be a list of date/times")
    if not isinstance(start, basestring):
        raise TypeError("start date/time should be a string in the ISO 8601 format")
    if not isinstance(end, basestring):
        raise TypeError("end date/time should be a string in the ISO 8601 format")

    from sparktk.frame.frame import Frame
    return Frame(self._tc,
                 self._scala.timeSeriesSlice(self._tc.jutils.convert.to_scala_date_time_list(date_time_index),
                                             self._tc.jutils.convert.to_scala_date_time(start),
                                             self._tc.jutils.convert.to_scala_date_time(end)))
