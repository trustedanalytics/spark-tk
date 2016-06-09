##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
    Usage:  python2.7 entropy_test.py
    Test Shannon entropy calculations
"""
__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart"]
__version__ = "2015.02.09"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import math

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class EntropyTest(atk_test.ATKTestCase):

    def setUp(self):
        super(EntropyTest, self).setUp()

        example_data = "example_count_v2.csv"
        example_schema = [("index", ia.int32)]
        self.frame = frame_utils.build_frame(
            example_data, example_schema, self.prefix)

    def test_entropy_coin_flip(self):
        """ Get entropy on balanced coin flip. """

        frame_load = 10 * ['H', 'T']
        expected = math.log(2.0)

        self.frame.filter(lambda (row): row.index in range(len(frame_load)))

        def complete_data(row):
            return frame_load[row.index]

        self.frame.add_columns(complete_data, [("data", str)])
        computed_entropy = self.frame.entropy("data")
        self.assertAlmostEqual(
            computed_entropy, expected,
            "Entropy of coin flip was %f; expected ln(2)" % computed_entropy)

    def test_entropy_exponential(self):
        """ Get entropy on exponential distribution. """

        frame_load = [(0, 1), (1, 2), (2, 4), (4, 8)]
        # Expected result is from an on-line entropy calculator in base 2.
        expected = 1.640223928941852 * math.log(2)

        self.frame.filter(lambda (row): row.index in range(len(frame_load)))

        self.frame.add_columns(lambda (row): frame_load[row.index],
                               [("data", ia.int32), ("weight", ia.int32)])

        computed_entropy = self.frame.entropy("data", "weight")
        print computed_entropy, expected
        self.assertAlmostEqual(computed_entropy, expected)


if __name__ == '__main__':
    unittest.main()
