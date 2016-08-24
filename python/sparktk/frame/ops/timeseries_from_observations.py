def timeseries_from_observations(self, date_time_index, timestamp_column, key_column, value_column):
    """
    Returns a frame that has the observations formatted as a time series.

    :param date_time_index: List of date/time strings. DateTimeIndex to conform all series to.
    :param timestamp_column: The name of the column telling when the observation occurred.
    :param key_column: The name of the column that contains which string key the observation belongs to.
    :param value_column: The name of the column that contains the observed value.
    :return: Frame formatted as a time series (with a column for key and a column for the vector of values).

    Uses the specified timestamp, key, and value columns and the date/time index provided to format the observations
    as a time series.  The time series frame will have columns for the key and a vector of the observed values that
    correspond to the date/time index.

    Examples
    --------
    In this example, we will use a frame of observations of resting heart rate for three individuals over three days.
    The data is accessed from Frame object called *my_frame*:

    <hide>
    >>> from datetime import datetime
    >>> data = [["Edward", "2016-01-01T12:00:00Z", 62],["Stanley", "2016-01-01T12:00:00Z", 57],["Edward", "2016-01-02T12:00:00Z", 63],["Sarah", "2016-01-02T12:00:00Z", 64],["Stanley", "2016-01-02T12:00:00Z", 57],["Edward", "2016-01-03T12:00:00Z", 62],["Sarah", "2016-01-03T12:00:00Z", 64],["Stanley", "2016-01-03T12:00:00Z", 56]]
    >>> schema = [("name", str), ("date", datetime), ("resting_heart_rate", float)]

    >>> my_frame = tc.frame.create(data, schema)
    -etc-

    </hide>

        >>> my_frame.inspect(my_frame.count())
        [#]  name     date                  resting_heart_rate
        ======================================================
        [0]  Edward   2016-01-01T12:00:00Z                  62
        [1]  Stanley  2016-01-01T12:00:00Z                  57
        [2]  Edward   2016-01-02T12:00:00Z                  63
        [3]  Sarah    2016-01-02T12:00:00Z                  64
        [4]  Stanley  2016-01-02T12:00:00Z                  57
        [5]  Edward   2016-01-03T12:00:00Z                  62
        [6]  Sarah    2016-01-03T12:00:00Z                  64
        [7]  Stanley  2016-01-03T12:00:00Z                  56

    We then need to create an array that contains the date/time index,
    which will be used when creating the time series.  Since our data
    is for three days, our date/time index will just contain those
    three dates:

        >>> datetimeindex = ["2016-01-01T12:00:00.000Z","2016-01-02T12:00:00.000Z","2016-01-03T12:00:00.000Z"]

    Then we can create our time series frame by specifying our date/time
    index along with the name of our timestamp column (in this example, it's
     "date"), key column (in this example, it's "name"), and value column (in
    this example, it's "resting_heart_rate").

         >>> ts = my_frame.timeseries_from_observations(datetimeindex, "date", "name", "resting_heart_rate")
         <progress>

    Take a look at the resulting time series frame schema and contents:

         >>> ts.schema
         [(u'name', <type 'unicode'>), (u'resting_heart_rate', vector(3))]

         >>> ts.inspect()
         [#]  name     resting_heart_rate
         ================================
         [0]  Stanley  [57.0, 57.0, 56.0]
         [1]  Edward   [62.0, 63.0, 62.0]
         [2]  Sarah    [None, 64.0, 64.0]



    """
    if not isinstance(date_time_index, list):
        raise TypeError("date_time_index should be a list of date/times")

    scala_date_list = self._tc.jutils.convert.to_scala_date_time_list(date_time_index)
    from sparktk.frame.frame import Frame
    return Frame(self._tc,
                 self._scala.timeSeriesFromObseravations(scala_date_list, timestamp_column, key_column, value_column))
