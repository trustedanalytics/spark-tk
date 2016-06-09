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
    Usage:  python2.7 dot_product_100element_test.py
    Builds a frame from 2 vectors (each with 100 elements) and the dot_product
    of each, then computes the dot_product() of each and adds a column with
    results which are then compared to the pre-computed values.
"""

__author__ = 'Grayson Churchel'
__version__ = "2015-04-06A"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class DotProduct100ElementTest(atk_test.ATKTestCase):

    def setUp(self):
        super(DotProduct100ElementTest, self).setUp()
        dataset = "dot_prod_100D_vect_36Rows.csv"
        schema = [("Vect_A", ia.vector(100)),
                  ("Vect_B", ia.vector(100)),
                  ("Base", ia.float64)]
        self.frame = frame_utils.build_frame(
            dataset, schema, self.prefix, skip_header_lines=1)

    def test_dot_product_100elements(self):
        self.frame.dot_product(["Vect_A"],
                               ["Vect_B"],
                               "Dot_prod")
        # Now pull all rows from the frame into 'results'.
        results = self.frame.take(self.frame.row_count)

        # Compare the pre-computed value of each to newly computed value.
        # ** Due to the variance in magnitude of values the variance allowed
        # is based on fractional size of the dot_product value. **
        for row in results:
            self.assertLess(abs(row[2] - row[3]), abs(row[2] / 1e11),
                            "Calculated: %s, is off by: %s" %
                            (row[3], row[2] - row[3]))

if __name__ == "__main__":
    unittest.main()
