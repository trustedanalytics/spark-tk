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

class RegressionTestMetrics(PropertiesObject):
    """
    RegressionMetrics class used to hold the data returned from regression tests
    """
    def __init__(self, scala_result):
        self._explained_variance_score = scala_result.explainedVarianceScore()
        self._mean_absolute_error = scala_result.meanAbsoluteError()
        self._mean_squared_error = scala_result.meanSquaredError()
        self._r2 = scala_result.r2()
        self._root_mean_squared_error = scala_result.rootMeanSquaredError()

    @property
    def explained_variance_score(self):
        """The explained variance regression score whose best possible value is 1"""
        return self._explained_variance_score

    @property
    def mean_absolute_error(self):
        """The risk function corresponding to the expected value of the absolute error loss or l1-norm loss"""
        return self._mean_absolute_error

    @property
    def mean_squared_error(self):
        """The risk function corresponding to the expected value of the squared error loss or quadratic loss"""
        return self._mean_squared_error

    @property
    def r2(self):
        """The coefficient of determination"""
        return self._r2

    @property
    def root_mean_squared_error(self):
        """The square root of the mean squared error"""
        return self._root_mean_squared_error
