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
        >>> columns=['Date', 'Time', 'CO_GT', 'PT08_S1_CO', 'NMHC_GT', 'C6H6_GT', 'Temp']
        >>> data = [['10/03/2004', '18.00.00', 2.6, 1360, 150, 11.9, 13.6],
        ...         ['10/03/2004', '19.00.00', 2.0, 1292, 112, 9.4, 13.3],
        ...         ['10/03/2004', '20.00.00', 2.2, 1402, 88, 9.0, 11.9],
        ...         ['10/03/2004', '21.00.00', 2.2, 1376, 80, 9.2, 11.0],
        ...         ['10/03/2004', '22.00.00', 1.6, 1272, 51, 6.5, 11.2],
        ...         ['10/03/2004', '23.00.00', 1.2, 1197, 38, 4.7, 11.2],
        ...         ['11/03/2004', '00.00.00', 1.2, 1185, 31, 3.6, 11.3],
        ...         ['11/03/2004', '01.00.00', 1.0, 1136, 31, 3.3, 10.7],
        ...         ['11/03/2004', '02.00.00', 0.9, 1094, 24, 2.3, 10.7],
        ...         ['11/03/2004', '03.00.00', 0.6, 1010, 19, 1.7, 10.3]]
        >>> frame = tc.frame.create(data=data, schema=columns)
    </hide>

    Consider the following frame that uses a snippet of air quality and sensor data from:

    https://archive.ics.uci.edu/ml/datasets/Air+Quality.

    Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
    Irvine, CA: University of California, School of Information and Computer Science.

        >>> frame.inspect()
        [#]  Date        Time      CO_GT  PT08_S1_CO  NMHC_GT  C6H6_GT  Temp
        ====================================================================
        [0]  10/03/2004  18.00.00    2.6        1360      150     11.9  13.6
        [1]  10/03/2004  19.00.00    2.0        1292      112      9.4  13.3
        [2]  10/03/2004  20.00.00    2.2        1402       88      9.0  11.9
        [3]  10/03/2004  21.00.00    2.2        1376       80      9.2  11.0
        [4]  10/03/2004  22.00.00    1.6        1272       51      6.5  11.2
        [5]  10/03/2004  23.00.00    1.2        1197       38      4.7  11.2
        [6]  11/03/2004  00.00.00    1.2        1185       31      3.6  11.3
        [7]  11/03/2004  01.00.00    1.0        1136       31      3.3  10.7
        [8]  11/03/2004  02.00.00    0.9        1094       24      2.3  10.7
        [9]  11/03/2004  03.00.00    0.6        1010       19      1.7  10.3


    Calcuate the Breusch-Godfrey test result:

        >>> y_column = "Temp"
        >>> x_columns = ['CO_GT', 'PT08_S1_CO', 'NMHC_GT', 'C6H6_GT']
        >>> max_lag = 1

        >>> result = frame.timeseries_breusch_godfrey_test(y_column, x_columns, max_lag)

        >>> result
        p_value   = 0.00353847462468
        test_stat = 8.50666768455

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