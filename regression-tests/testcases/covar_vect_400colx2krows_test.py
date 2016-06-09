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
    Usage:  python2.7 Covar_vect_400ColX2KRows_test.py
    Tests covariance on a frame built from a vector.
    Computes covariance on 2048 vectors of 400 elements each.
"""

__author__ = 'Grayson Churchel'
__version__ = "2015-04-06A"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import trustedanalytics as ia

import unittest

from qalib import atk_test
from qalib import common_utils
from qalib import frame_utils


class CovarVect400ColX2KRowTest(atk_test.ATKTestCase):
    """Verifies that we can build a frame from vectors and then compute
    the covariance values based on the 'columns' in those vectors rather
    than using individual data columns."""

    def setUp(self):
        """Establish values used in the test."""
        super(CovarVect400ColX2KRowTest, self).setUp()

        input_file = "Covar_vector_400Elem_2KRows.csv"
        self.reference_file = "Covar_vect_400sq_baseline_v01.txt"
        vect_schema = [("items", ia.vector(400))]

        self.cov_frame = frame_utils.build_frame(
            input_file, vect_schema, prefix=self.prefix)

    def test_covar_on_vectors(self):
        """Compute a covariance matrix based on the 400 elements in
        each vector and then compare those against a 400 by 400 array
        of values which were previously computed in Python."""
        cov_matrix = self.cov_frame.covariance_matrix(['items'])

        # Create schema for 400-column reference file/frame.
        sch2 = [("col_" + str(i), ia.float64) for i in range(1, 401)]
        # Build a frame from the pre-computed values for this dataset.
        ref_frame = frame_utils.build_frame(
            self.reference_file, sch2, self.prefix)
        # Then flatten it to a long list of values.
        baseline_flat = common_utils.flatten(ref_frame.take
                                             (ref_frame.row_count))
        # Flatten the frame created by computing covariance on the vectors.
        computed_flat = common_utils.flatten(cov_matrix.take
                                             (cov_matrix.row_count))

        # Now loop through all 160,000 values and verify they're acceptable.
        for base, computed in zip(baseline_flat, computed_flat):
            self.assertAlmostEqual(computed, base, 7,
                                   "Expected %f but got %f" % (base, computed))

if __name__ == "__main__":
    unittest.main()
