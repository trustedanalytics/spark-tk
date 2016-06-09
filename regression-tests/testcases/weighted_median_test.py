##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
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
# ##############################################################################
"""
usage:
python2.7 weighted_median.py

Tests weighted median functionality.
Calculates the median for a column of a frame containing numerical values.
Compares against known values.
"""

__author__ = "lcoatesx"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class WeightedMedians(atk_test.ATKTestCase):

    def setUp(self):
        """Create input and baselines before run."""
        super(WeightedMedians, self).setUp()
        self.dataset1 = "weight_median.csv"
        self.dataset2 = "weighted_median_negative.csv"

        self.schema1 = [("col_1", ia.int32), ("col_2", ia.int32)]
        self.schema2 = [("col_1", ia.int32), ("col_2", ia.int32),
                        ("col_3", ia.int32)]

        # The following values are calculated manually and are used to
        # compare the results with the one found after execution.
        self.dataset_1_result1 = 499
        self.dataset_1_result2 = 707

        self.dataset_2_result1 = 49999
        self.dataset_2_result2 = 29289
        self.dataset_2_result3 = None

        # Big data frame creation
        self.frame_median = frame_utils.build_frame(
            self.dataset1, self.schema1, self.prefix)
        # Big data frame creation
        self.frame_median2 = frame_utils.build_frame(
            self.dataset2, self.schema2, self.prefix)

    def test_non_weighted_median(self):
        """Non weighted median calculation on smaller dataset"""
        weighted_median = self.frame_median.column_median('col_1')
        self.assertEqual(weighted_median, self.dataset_1_result1)

    def test_weighted_median_small_data(self):
        """Weighted median on smaller dataset"""
        weighted_median = self.frame_median.column_median('col_1', 'col_2')
        self.assertEqual(weighted_median, self.dataset_1_result2)

    def test_non_weighted_large_data(self):
        """ Non weighted median calculation on larger dataset"""
        weighted_median = self.frame_median2.column_median('col_1')
        self.assertEqual(weighted_median, self.dataset_2_result1)

    def test_weighted_median_large_data(self):
        """Weighted median on larger dataset"""
        weighted_median = self.frame_median2.column_median('col_1', 'col_2')
        self.assertEqual(weighted_median, self.dataset_2_result2)

    def test_weighted_median_negative_weights(self):
        """Weighted median calculation where weights are negative integers"""
        weighted_median = self.frame_median2.column_median('col_1', 'col_3')
        self.assertEqual(weighted_median, None)

if __name__ == '__main__':
    unittest.main()
