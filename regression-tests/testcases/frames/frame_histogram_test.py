"""Tests the histogram functionality"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class FrameHistogramTest(sparktk_test.SparkTKTestCase):

    def test_histogram_standard(self):
        """Tests the default behavior of histogram."""
        histogram_file = self.get_file("histogram.csv")

        schema = [("value", int)]

        # Big data frame from data with 33% correct predicted ratings
        self.frame_histogram = self.context.frame.import_csv(histogram_file, schema=schema)
        result = self.frame_histogram.histogram("value", num_bins=10)

        # verified known results based on data crafted
        cutoffs = [1.0, 1.9, 2.8, 3.7, 4.6, 5.5, 6.4, 7.3, 8.2, 9.1, 10.0]
        histogram = [10.0, 10.0, 10.0, 10.0, 10.0,
                     10.0, 10.0, 10.0, 10.0, 10.0]
        density = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]

        self.assertAlmostEqual(list(cutoffs), list(result.cutoffs))
        self.assertAlmostEqual(list(histogram), list(result.hist))
        self.assertAlmostEqual(list(density), list(result.density))


if __name__ == '__main__':
    unittest.main()
