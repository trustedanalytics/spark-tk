from sparktk.propobj import PropertiesObject

def timeseries_breusch_godfrey_test(self, residuals, factors, max_lag):
    """
    Calculates the Breusch-Godfrey test statistic for serial correlation.

    Parameters
    ----------

    :param residuals: (str) Name of the column that contains residual (y) values
    :param factors: (List[str]) Name of the column(s) that contain factors (x) values
    :param max_lag: (int) The lag order to calculate the test statistic.
    :return: (BreuschGodfreyTestResult) Object contains the Breusch-Godfrey test statistic and p-value.

    Example
    -------

    <hide>
        >>> columns=["y","num_attendees", "weekend_flag", "seasonality", "min_temp","max_temp"]
        >>> frame = tc.frame.create([[100,465,1,0.006562479,24,51],
        ...                          [98,453,1,0.00643123,24,54],
        ...                          [102,472,0,0.006693729,25,49],
        ...                          [98,454,0,0.00643123,25,46],
        ...                          [112,432,0,0.007349977,25,42],
        ...                          [99,431,0,0.006496855,25,41],
        ...                          [99,475,0,0.006496855,25,45],
        ...                          [87,393,1,0.005709357,25,46],
        ...                          [103,437,1,0.006759354,25,48]], schema=columns)
    </hide>

    Consider the following frame:

        >>> frame.inspect()
        [#]  y    num_attendees  weekend_flag  seasonality  min_temp  max_temp
        ======================================================================
        [0]  100            465             1  0.006562479        24        51
        [1]   98            453             1   0.00643123        24        54
        [2]  102            472             0  0.006693729        25        49
        [3]   98            454             0   0.00643123        25        46
        [4]  112            432             0  0.007349977        25        42
        [5]   99            431             0  0.006496855        25        41
        [6]   99            475             0  0.006496855        25        45
        [7]   87            393             1  0.005709357        25        46
        [8]  103            437             1  0.006759354        25        48

    Calcuate the Breusch-Godfrey test result:

        >>> y_column = "y"
        >>> x_columns = ["num_attendees", "weekend_flag", "seasonality", "min_temp","max_temp"]
        >>> max_lag = 1

        >>> result = frame.timeseries_breusch_godfrey_test(y_column, x_columns, max_lag)

        >>> result
        p_value   = 0.00467773498105
        test_stat = 8.0



    """
    if not isinstance(residuals, str):
        raise TypeError("residuals parameter should be a str (column name).")
    if isinstance(factors, str):
        factors = [factors]
    if not isinstance(factors, list):
        raise TypeError("factors parameter should be a list of strings (column names).")
    if not isinstance(max_lag, int):
        raise TypeError("max_lag parameter should be an integer.")

    scala_result = self._scala.timeSeriesBreuschGodfreyTest(residuals,
                                                      self._tc.jutils.convert.to_scala_list_string(factors),
                                                      max_lag)
    return BreuschGodfreyTestResult(scala_result)

class BreuschGodfreyTestResult(PropertiesObject):
    """
    BreuschGodfreyTestResult class contains values that are returned from the breusch_godfrey_test.
    """
    def __init__(self, scala_result):
        self._test_stat = scala_result.testStat()
        self._p_value = scala_result.pValue()

    @property
    def test_stat(self):
        """
        Breusch-Godfrey test statistic
        """
        return self._test_stat

    @property
    def p_value(self):
        """
        p-value
        """
        return self._p_value