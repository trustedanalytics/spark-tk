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

from sparktk.propobj import PropertiesObject

def timeseries_breusch_pagan_test(self, residuals, factors):
    """
    Peforms the Breusch-Pagan test for heteroskedasticity.

    Parameters
    ----------

    :param residuals: (str) Name of the column that contains residual (y) values
    :param factors: (List[str]) Name of the column(s) that contain factors (x) values
    :return: (BreuschPaganTestResult) Object contains the Breusch-Pagan test statistic and p-value.

    Example
    -------

    <hide>
        >>> data = [[8.34,40.77,1010.84,90.01,480.48],
        ...         [23.64,58.49,1011.4,74.2,445.75],
        ...         [29.74,56.9,1007.15,41.91,438.76],
        ...         [19.07,49.69,1007.22,76.79,453.09],
        ...         [11.8,40.66,1017.13,97.2,464.43],
        ...         [13.97,39.16,1016.05,84.6,470.96],
        ...         [22.1,71.29,1008.2,75.38,442.35],
        ...         [14.47,41.76,1021.98,78.41,464],
        ...         [31.25,69.51,1010.25,36.83,428.77],
        ...         [6.77,38.18,1017.8,81.13,484.3],
        ...         [28.28,68.67,1006.36,69.9,435.29],
        ...         [22.99,46.93,1014.15,49.42,451.41],
        ...         [29.3,70.04,1010.95,61.23,426.25],
        ...         [8.14,37.49,1009.04,80.33,480.66],
        ...         [16.92,44.6,1017.34,58.75,460.17],
        ...         [22.72,64.15,1021.14,60.34,453.13],
        ...         [18.14,43.56,1012.83,47.1,461.71],
        ...         [11.49,44.63,1020.44,86.04,471.08],
        ...         [9.94,40.46,1018.9,68.51,473.74],
        ...         [23.54,41.1,1002.05,38.05,448.56],
        ...         [14.9,52.05,1015.11,77.33,464.82],
        ...         [33.8,64.96,1004.88,49.37,427.28],
        ...         [25.37,68.31,1011.12,70.99,441.76],
        ...         [7.29,41.04,1024.06,89.19,474.71]]
        >>> schema = [("AT", float),("V", float),("AP", float),("RH", float),("PE", float)]
        >>> frame = tc.frame.create(data, schema)
    </hide>

    Consider the following frame:

        >>> frame.inspect()
        [#]  AT     V      AP       RH     PE
        =========================================
        [0]   8.34  40.77  1010.84  90.01  480.48
        [1]  23.64  58.49   1011.4   74.2  445.75
        [2]  29.74   56.9  1007.15  41.91  438.76
        [3]  19.07  49.69  1007.22  76.79  453.09
        [4]   11.8  40.66  1017.13   97.2  464.43
        [5]  13.97  39.16  1016.05   84.6  470.96
        [6]   22.1  71.29   1008.2  75.38  442.35
        [7]  14.47  41.76  1021.98  78.41     464
        [8]  31.25  69.51  1010.25  36.83  428.77
        [9]   6.77  38.18   1017.8  81.13   484.3

    Calculate the Bruesh-Pagan test statistic where the "AT" column contains residual values and the other columns are
    factors:

    >>> result = frame.timeseries_breusch_pagan_test("AT",["V","AP","RH","PE"])
    <progress>

    The result contains the test statistic and p-value:

    >>> result
    p_value   = 0.000147089380721
    test_stat = 22.6741588802

    """
    if not isinstance(residuals, str):
        raise TypeError("residuals parameter should be a str (column name).")
    if isinstance(factors, str):
        factors = [factors]
    if not isinstance(factors, list):
        raise TypeError("factors parameter should be a list of strings (column names).")

    scala_result = self._scala.timeSeriesBreuschPaganTest(residuals,
                                                          self._tc.jutils.convert.to_scala_list_string(factors))
    return BreuschPaganTestResult(scala_result)

class BreuschPaganTestResult(PropertiesObject):
    """
    BreuschPaganTestResult class contains values that are returned from the breusch_Pagan_test.
    """
    def __init__(self, scala_result):
        self._test_stat = scala_result.testStat()
        self._p_value = scala_result.pValue()

    @property
    def test_stat(self):
        """
        Breusch-Pagan test statistic
        """
        return self._test_stat

    @property
    def p_value(self):
        """
        p-value
        """
        return self._p_value