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

"""Tests the quantile functionality"""

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test
from sparktk import dtypes


class FrameQuantileTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        schema = [("value", int)]
        self.frame_histogram = self.context.frame.import_csv(
            self.get_file("histogram.csv"), schema=schema)

    def test_histogram_standard(self):
        """Tests the default behavior of histogram."""
        result = self.frame_histogram.quantiles(
            "value", [5, 10, 30, 70, 75, 80, 95])
        values = result.take(result.count())

        # These values are known from the construction of the dataset
        correct_values = [[5.0, 1.0], [10.0, 1.0], [30.0, 3.0], [70.0, 7.0],
                          [75.0, 8.0], [80.0, 8.0], [95.0, 10.0]]
        self.assertItemsEqual(values, correct_values)

    @unittest.skip("Exception not raised by current impl")
    def test_histogram_no_quantiles(self):
        """Tests the behavior when no quantiles provided"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "value")

    @unittest.skip("Exception not raised by current impl")
    def test_histogram_exceed_quantile_range(self):
        """Tests the behaviour when a quantile > 100 is requested"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "values", [101])

    @unittest.skip("Exception not raised by current impl")
    def test_histogram_negative_quantiles(self):
        """Tests the behavior when negative quantiles provided"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "value", [-10, -5])

    def test_histogram_bad_quantiles(self):
        """Tests the behavior when string quantiles provided"""
        with self.assertRaisesRegexp(ValueError, "could not convert string to float"):
            self.frame_histogram.quantiles(
                "value", ['str'])
if __name__ == '__main__':
    unittest.main()
