from sparktk.propobj import PropertiesObject

def timeseries_augmented_dickey_fuller_test(self, ts_column, max_lag, regression = "c"):
    """
    Performs the Augmented Dickey-Fuller (ADF) Test, which tests the null hypothesis of whether a unit root is present
    in a time series sample. The test statistic that is returned in a negative number.  The lower the value, the
    stronger the rejection of the hypothesis that there is a unit root at some level of confidence.

    Parameters
    ----------

    :param ts_column: (str) Name of the column that contains the time series values to use with the ADF test.
    :param max_lag: (int) The lag order to calculate the test statistic.
    :param regression: (Optional(str)) The method of regression that was used. Following MacKinnon's notation, this
                       can be "c" for constant, "nc" for no constant, "ct" for constant and trend, and "ctt" for
                       constant, trend, and trend-squared.
    :return: (AugmentedDickeyFullerTestResult) Object contains the ADF test statistic and p-value.

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

    Consider the following frame of time series values:

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

    Calculate augmented Dickey-Fuller test statistic by giving it the name of the column that has the time series
    values and the max_lag.  The function returns an object that has properties for the p-value and test statistic.

        >>> frame.timeseries_augmented_dickey_fuller_test("timeseries_values", 0)
        p_value   = 3.3005431721e-11
        test_stat = -7.54504405591

    """

    if not isinstance(ts_column, str):
        raise TypeError("ts_column parameter should be a str")
    if not isinstance(max_lag, int):
        raise TypeError("max_lag parameter should be a int")
    if not isinstance(regression, str):
        raise TypeError("regression parameter should be a str")

    scala_result = self._scala.timeSeriesAugmentedDickeyFullerTest(ts_column, max_lag, regression)
    return AugmentedDickeyFullerTestResult(scala_result)

class AugmentedDickeyFullerTestResult(PropertiesObject):
    """
    AugmentedDickeyFullerTestResult class contains values that are returned from the augmented_dickey_fuller_test.
    """

    def __init__(self, scala_result):
        self._test_stat = scala_result.testStat()
        self._p_value = scala_result.pValue()

    @property
    def test_stat(self):
        """
        ADF test statistic
        """
        return self._test_stat

    @property
    def p_value(self):
        """
        p-value
        """
        return self._p_value