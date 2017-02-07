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

"""Tests the histogram functionality"""

import unittest
from sparktkregtests.lib import sparktk_test


class FrameHistogramTest(sparktk_test.SparkTKTestCase):

    def test_histogram_standard(self):
        """Tests the default behavior of histogram."""
        histogram_file = self.get_file("histogram.csv")

        schema = [("value", int)]

        # Big data frame from data with 33% correct predicted ratings
        self.frame_histogram = self.context.frame.import_csv(
            histogram_file, schema=schema)
        result = self.frame_histogram.histogram("value", num_bins=10)

        # verified known results based on data crafted
        cutoffs = [1.0, 1.9, 2.8, 3.7, 4.6, 5.5, 6.4, 7.3, 8.2, 9.1, 10.0]
        histogram = [10.0, 10.0, 10.0, 10.0, 10.0,
                     10.0, 10.0, 10.0, 10.0, 10.0]
        density = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]

        self.assertAlmostEqual(cutoffs, result.cutoffs)
        self.assertAlmostEqual(histogram, result.hist)
        self.assertAlmostEqual(density, result.density)


if __name__ == '__main__':
    unittest.main()
