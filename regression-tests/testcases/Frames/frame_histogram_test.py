##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################
"""
usage:
python2.7 frame_histogram_test.py

Tests the histogram functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class FrameHistogramTest(atk_test.ATKTestCase):

    def setUp(self):
        """
        Verify that the input and baselines exist before running the tests.
        """
        super(FrameHistogramTest, self).setUp()

        # Movie user data with original ratings
        histogram_file = "histogram.csv"

        schema = [("value", ia.int32)]

        # Big data frame from data with 33% correct predicted ratings
        self.frame_histogram = frame_utils.build_frame(
            histogram_file, schema, self.prefix)

    def test_histogram_standard(self):
        """Tests the default behavior of histogram."""
        result = self.frame_histogram.histogram("value", num_bins=10)

        # These cutoffs do make sense, but I am not certain how they are
        # calculated
        cutoffs = [1.0, 1.9, 2.8, 3.7, 4.6, 5.5, 6.4, 7.3, 8.2, 9.1, 10.0]

        # verified known results based on data crafted
        histogram = [10.0, 10.0, 10.0, 10.0, 10.0,
                     10.0, 10.0, 10.0, 10.0, 10.0]
        density = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]

        self.assertEqual(cutoffs, result.cutoffs)
        self.assertEqual(histogram, result.hist)
        self.assertEqual(density, result.density)


if __name__ == '__main__':
    unittest.main()
