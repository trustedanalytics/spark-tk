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

"""Tests frame timeseries tests """

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test

import random

import pandas
import numpy as np
import statsmodels.stats.stattools as smst
import statsmodels.tsa.stattools as smtsa
from scipy import stats
import statsmodels.api as sm
import statsmodels.stats.diagnostic as smd
import statsmodels.formula.api as smf
import statsmodels.regression.linear_model as smrl

#Change filename to the location of the csv file used for the test
filename = "../datasets/timeseriesstats.csv"

class FrameInspectTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameInspectTest, self).setUp()

        dataset = self.get_file("timeseriesstats.csv")
        schema = [("year", float),
                  ("logM", float),
                  ("logYp", float),
                  ("Rs", float),
                  ("Rl", float),
                  ("Rm", float),
                  ("logSpp", float)]

        self.frame = self.context.frame.import_csv(
            dataset, delimiter= ' ', header=True, schema=schema)
        self.pandaframe = self.frame.to_pandas()
        self.pandaframe.logM = self.pandaframe.logM.astype(np.float64)
        self.pandaframe.Rs = self.pandaframe.Rs.astype(np.float64)
        self.pandaframe.Rl = self.pandaframe.Rl.astype(np.float64)

    def test_frame_timeseries_durbin_watson(self):
        """Test Durbin Watson"""
        result = self.frame.timeseries_durbin_watson_test("logM")
        db_result = smst.durbin_watson(self.pandaframe["logM"])

        self.assertAlmostEqual(result, db_result, delta=0.0000000001)

    def test_frame_timeseries_dickey_fuller_constant(self):
        """Test Augmented Dickey Fuller with constant regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test("logM", max_lag=0, regression="c")
        df_c_result = smtsa.adfuller(self.pandaframe["logM"], maxlag=0, regression="c")

        self.assertAlmostEqual(result.p_value, df_c_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_c_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_no_constant(self):
        """Test Augmented Dickey Fuller with no constant regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test("logM", max_lag=1, regression="nc")
        df_nc_result = smtsa.adfuller(self.pandaframe["logM"], maxlag=1, regression="nc")

        self.assertAlmostEqual(result.p_value, df_nc_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_nc_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_constant_and_trend(self):
        """Test Augmented Dickey Fuller with constant and trend regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test("logM", max_lag=1, regression="ct")
        df_ct_result = smtsa.adfuller(self.pandaframe["logM"], maxlag=1, regression="ct")

        self.assertAlmostEqual(result.p_value, df_ct_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_ct_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_constant_trend_squared(self):
        """Test Augmented Dickey Fuller with constant, trend, and trend squared regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test("logM", max_lag=1, regression="ctt")
        df_ctt_result = smtsa.adfuller(self.pandaframe["logM"], maxlag=1, regression="ctt")

        self.assertAlmostEqual(result.p_value, df_ctt_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_ctt_result[0], delta=0.01)

if __name__ == "__main__":
    unittest.main()
