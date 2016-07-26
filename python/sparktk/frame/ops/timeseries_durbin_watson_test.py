
def timeseries_durbin_watson_test(self, residuals):
    """
    Computes the Durbin-Watson test statistic used to determine the presence of serial correlation in the residuals.
    Serial correlation can show a relationship between values separated from each other by a given time lag. A value
    close to 0.0 gives evidence for positive serial correlation, a value close to 4.0 gives evidence for negative
    serial correlation, and a value close to 2.0 gives evidence for no serial correlation.

    :param residuals: (str) Name of the column that contains residual values
    :return: Durbin-Watson statistics test

    Example
    -------

    <hide>
        >>> data = [3.201,3.3178,3.6279,3.5902,3.43,4.0546,3.7606,3.1231,3.2077,4.3383,3.1658,3.5846,
        ...         3.7396,3.9033,4.4777,3.7152,4.4831,3.2167,4.12,4.2985,3.8357,4.3181,4.2623,3.7079,
        ...         3.599,3.811,3.6797, 4.1362, 3.7926,3.4588,3.6344,3.5755,3.7222,3.3418,3.8089,3.8528,
        ...         3.4912,3.0793,4.2237,4.4493,3.3707,3.1553,3.5583,4.7962, 3.2689,4.3124,3.2563,3.0801,
        ...         3.8196,3.3082,3.2765,3.8451,3.29,4.3531,3.0146,3.5044,3.4059,3.5999,3.2379,4.5741,3.4916,
        ...         3.8506,3.2511,3.7444,4.06,4.4784,3.7102,3.5334,3.9951,4.1823,3.7677,4.0008,3.9842,3.4518,
        ...         3.7062,4.7776,3.8842,3.6932,4.0672,3.7259,3.6426,3.6229,4.6453,4.0996,4.0615,3.3162,3.841]
        >>> frame = tc.frame.create([[value] for value in data], ["timeseries_values"])
    </hide>

    In this example, we have a frame that contains time series values.  The inspect command below shows a snippet of
    what the data looks like:

        >>> frame.inspect()
        [#]  timeseries_values
        ======================
        [0]              3.201
        [1]             3.3178
        [2]             3.6279
        [3]             3.5902
        [4]               3.43
        [5]             4.0546
        [6]             3.7606
        [7]             3.1231
        [8]             3.2077
        [9]             4.3383

    Calculate Durbin-Watson test statistic by giving it the name of the column that has the time series values:

        >>> frame.timeseries_durbin_watson_test("timeseries_values")
        0.02678674777710402

    """

    if not isinstance(residuals, str):
        raise TypeError("residuals should be a str (column name).")

    return self._scala.timeSeriesDurbinWatsonTest(residuals)