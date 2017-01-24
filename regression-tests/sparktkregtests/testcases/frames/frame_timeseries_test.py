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
from sparktkregtests.lib import sparktk_test

import numpy as np
import statsmodels.stats.stattools as smst
import statsmodels.tsa.stattools as smtsa
from sklearn import linear_model


class FrameTimeseriesTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameTimeseriesTest, self).setUp()

        dataset = self.get_file("timeseriesstats.csv")
        schema = [("year", float),
                  ("logM", float),
                  ("logYp", float),
                  ("Rs", float),
                  ("Rl", float),
                  ("Rm", float),
                  ("logSpp", float)]

        self.frame = self.context.frame.import_csv(
            dataset, delimiter=' ', header=True, schema=schema)
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
        result = self.frame.timeseries_augmented_dickey_fuller_test(
            "logM", max_lag=0, regression="c")
        df_c_result = smtsa.adfuller(
            self.pandaframe["logM"], maxlag=0, regression="c")

        self.assertAlmostEqual(result.p_value, df_c_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_c_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_no_constant(self):
        """Test Augmented Dickey Fuller with no constant regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test(
            "logM", max_lag=1, regression="nc")
        df_nc_result = smtsa.adfuller(
            self.pandaframe["logM"], maxlag=1, regression="nc")

        self.assertAlmostEqual(result.p_value, df_nc_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_nc_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_constant_and_trend(self):
        """Test Augmented Dickey Fuller with constant and trend regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test(
            "logM", max_lag=1, regression="ct")
        df_ct_result = smtsa.adfuller(
            self.pandaframe["logM"], maxlag=1, regression="ct")

        self.assertAlmostEqual(result.p_value, df_ct_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_ct_result[0], delta=0.01)

    def test_frame_timeseries_dickey_fuller_constant_trend_squared(self):
        """Augmented Dickey Fuller constant, trend, trend squared regression"""
        result = self.frame.timeseries_augmented_dickey_fuller_test(
            "logM", max_lag=1, regression="ctt")
        df_ctt_result = smtsa.adfuller(
            self.pandaframe["logM"], maxlag=1, regression="ctt")

        self.assertAlmostEqual(result.p_value, df_ctt_result[1], delta=0.0001)
        self.assertAlmostEqual(result.test_stat, df_ctt_result[0], delta=0.01)

    def test_frame_timeseries_breusch_pagan(self):
        """Test Breusch Pagan
            using the regression determined according to page 5 of this
            paper: http://people.stfx.ca/tleo/econ370term2lec1.pdf"""
        dataset = self.get_file("breusch.csv")
        schema = [("time", float),
                  ("heteroskedastic", float),
                  ("sine", float),
                  ("uniform", float)]

        frame = self.context.frame.import_csv(
            dataset, delimiter=' ', header=True, schema=schema)
        het_result = frame.timeseries_breusch_pagan_test(
            "heteroskedastic", "time")
        uni_result = frame.timeseries_breusch_pagan_test("uniform", "time")

        time = frame.take(frame.count(), columns=['time'])

        het_data = frame.take(frame.count(), columns="heteroskedastic")
        het_residual = [item**2 for sublist in het_data for item in sublist]
        het_reg = linear_model.LinearRegression()
        hetOLS = het_reg.fit(time, het_residual)
        het_stat = hetOLS.score(time, het_residual) * frame.count()

        uni_data = frame.take(frame.count(), columns="uniform")
        uni_residual = [item**2 for sublist in uni_data for item in sublist]
        reg2 = linear_model.LinearRegression()
        uniOLS = reg2.fit(time, uni_residual)
        uni_stat = uniOLS.score(time, uni_residual) * frame.count()

        self.assertLess(het_result.p_value, 0.05)
        self.assertAlmostEqual(het_result.test_stat, het_stat)
        self.assertGreater(uni_result.p_value, 0.05)
        self.assertAlmostEqual(uni_result.test_stat, uni_stat)

    def test_frame_timeseries_breusch_godfrey(self):
        """Test Breusch Godfrey"""
        dataset = self.get_file("breusch.csv")
        schema = [("time", float),
                  ("heteroskedastic", float),
                  ("sine", float),
                  ("uniform", float)]
        max_lag = 1
        frame = self.context.frame.import_csv(
            dataset, delimiter=' ', header=True, schema=schema)
        sine_result = frame.timeseries_breusch_godfrey_test(
            "sine", ['time'], max_lag)
        uni_result = frame.timeseries_breusch_godfrey_test(
            "uniform", ['time'], max_lag)

        time_data = frame.take(frame.count(), columns=['time'])
        time = [item for sublist in time_data for item in sublist]

        sine_data = frame.take(frame.count(), columns="sine")
        sine_residual = [item for sublist in sine_data for item in sublist]
        lagged_sine_residuals = [0]+sine_residual[:-1]
        sine_factors = np.column_stack((time, lagged_sine_residuals))
        sine_reg = linear_model.LinearRegression()
        sineOLS = sine_reg.fit(sine_factors, sine_residual)
        sine_stat = (frame.count()-max_lag)*sineOLS.score(
            sine_factors, sine_residual)

        uni_data = frame.take(frame.count(), columns="uniform")
        uni_residual = [item for sublist in uni_data for item in sublist]
        lagged_uni_residuals = [0]+uni_residual[:-1]
        uni_factors = np.column_stack((time, lagged_uni_residuals))
        uni_reg = linear_model.LinearRegression()
        uniOLS = uni_reg.fit(uni_factors, uni_residual)
        uni_stat = (frame.count()-max_lag)*uniOLS.score(
            uni_factors, uni_residual)

        self.assertLess(sine_result.p_value, 0.05)
        self.assertAlmostEqual(sine_result.test_stat, sine_stat, delta=1)
        self.assertGreater(uni_result.p_value, 0.05)
        self.assertAlmostEqual(uni_result.test_stat, uni_stat, delta=1)


if __name__ == "__main__":
    unittest.main()
