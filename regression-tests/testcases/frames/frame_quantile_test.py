"""Tests the quantile functionality"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.realpath(__file__)))))
from qalib import sparktk_test
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
        values = result.take(result.row_count).data

        # These values are known from the construction of the dataset
        correct_values = [[5.0, 1.0], [10.0, 1.0], [30.0, 3.0], [70.0, 7.0],
                          [75.0, 8.0], [80.0, 8.0], [95.0, 10.0]]
        self.assertItemsEqual(values, correct_values)

    def test_histogram_no_quantiles(self):
        """Tests the behavior when no quantiles provided"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "value")

    def test_histogram_exceed_quantile_range(self):
        """Tests the behaviour when a quantile > 100 is requested"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "values", [101])

    def test_histogram_negative_quantiles(self):
        """Tests the behavior when negative quantiles provided"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "value", [-10, -5])

    def test_histogram_bad_quantiles(self):
        """Tests the behavior when string quantiles provided"""
        with self.assertRaises(Exception):
            self.frame_histogram.quantiles(
                "value", ['str'])
if __name__ == '__main__':
    unittest.main()
