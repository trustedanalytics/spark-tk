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
"""tests daal covariance code"""

import unittest

import trustedanalytics as ia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class CovarianceCorrelationTest(atk_test.ATKTestCase):

    def setUp(self):
        """ Assign data files and schemas """
        super(CovarianceCorrelationTest, self).setUp()

        self.data_in = "Covariance_400Col_1K.02.csv"
        self.covar_result = "Covariance_400sq_ref.02.txt"
        self.correl_result = "Correlation_400sq_ref.02.txt"

        self.schema_in = []
        for i in range(1, 301):
            col_spec = ('col_' + str(i), ia.int64)
            self.schema_in.append(col_spec)
        for i in range(301, 401):
            col_spec = ('col_' + str(i), ia.float64)
            self.schema_in.append(col_spec)
        # print self.schema_in

        self.schema_result = []
        for i in range(1, 401):
            col_spec = ('col_' + str(i), ia.float64)
            self.schema_result.append(col_spec)

        # Define a list of column names to be used in creating the covariance
        # and correlation matrices.
        self.columns = ["col_"+str(i) for i in range(1, 401)]
        self.base_frame = frame_utils.build_frame(self.data_in, self.schema_in)


    def test_covar_matrix(self):
        # Verifies the covariance_matrix function on all 400 columns.
        covar_ref_frame = frame_utils.build_frame(
            self.covar_result, self.schema_result)
        covar_matrix = self.base_frame.daal_covariance_matrix(self.columns)
        covar_flat = common_utils.flatten(covar_matrix.take
                                          (covar_matrix.row_count))
        covar_ref_flat = common_utils.flatten(covar_ref_frame.take
                                              (covar_ref_frame.row_count))
        pos = 0
        for cov_value, res_value in zip(covar_flat, covar_ref_flat):
            # print "compare", cov_value, "to", res_value
            self.assertAlmostEqual(
                cov_value, res_value, 7,
                "%g != %g in flattened position #%d" %
                (cov_value, res_value, pos))
            pos += 1


if __name__ == "__main__":
    unittest.main()
