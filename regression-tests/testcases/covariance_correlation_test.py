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
    Usage:  python2.7 covariance_correlation_test.py
    Tests covariance and correlation on 2 columns each,
    and covariance_matrix and correlation_matrix on a
    400 column by 1024 row dataset.

"""

__author__ = 'Grayson Churchel'
__credits__ = ["Grayson Churchel", "Prune Wickart"]
__version__ = "20150213.1"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class CovarianceCorrelationTest(atk_test.ATKTestCase):

    def setUp(self):
        """
        Assign data files and schemas
        """
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

    def test_covar(self):
        # Tests covariance function on two columns in dataset.
        covar_2_5 = self.base_frame.covariance(['col_2', 'col_5'])
        self.assertEqual(covar_2_5, -5.25513196480937949673,
                         "Covariance value %30.28f not equal to reference."
                         % covar_2_5)

    def test_correl(self):
        # Test correlation function on two columns in dataset.
        correl_1_3 = self.base_frame.correlation(['col_1', 'col_3'])
        self.assertEqual(correl_1_3, 0.124996244847393425669857,
                         "Correlation value %30.29f not equal to reference."
                         % correl_1_3)

    def test_covar_matrix(self):
        # Verifies the covariance_matrix function on all 400 columns.
        covar_ref_frame = frame_utils.build_frame(
            self.covar_result, self.schema_result)
        covar_matrix = self.base_frame.covariance_matrix(self.columns)
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

    def test_correl_matrix(self):
        # Verifies the correlation_matrix function on all 400 columns.
        correl_matrix = self.base_frame.correlation_matrix(self.columns)
        correl_ref_frame = frame_utils.build_frame(
            self.correl_result, self.schema_result)
        correl_flat = common_utils.flatten(correl_matrix.take
                                           (correl_matrix.row_count))
        cor_ref_flat = common_utils.flatten(correl_ref_frame.take
                                            (correl_ref_frame.row_count))
        pos = 0
        for correl_value, ref_value in zip(correl_flat, cor_ref_flat):
            # print "Comparing ", correlation value, " to ", reference value
            self.assertAlmostEqual(
                correl_value, ref_value, 5,
                "%g != %g in flattened position #%d" %
                (correl_value, ref_value, pos))
            # Correlation_matrix function is only accurate to 5 decimal places.
            pos += 1


if __name__ == "__main__":
    unittest.main()
