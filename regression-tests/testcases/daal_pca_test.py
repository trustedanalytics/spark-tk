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

''' test cases for Pricipal Components Analysis'''
# usage: python2.7 pca_test.py
# Tests Principal Components Analysis against known values
# calculated using numpy svd.

import sys
import os
sys.path.append(os.path.realpath(__file__)+"..")

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ta

from qalib import frame_utils
from qalib import atk_test


class DaalPrincipalComponent(atk_test.ATKTestCase):

    def setUp(self):
        super(DaalPrincipalComponent, self).setUp()
        self.schema1 = [("X1", ta.int32),
                        ("X2", ta.int32),
                        ("X3", ta.int32),
                        ("X4", ta.int32),
                        ("X5", ta.int32),
                        ("X6", ta.int32),
                        ("X7", ta.int32),
                        ("X8", ta.int32),
                        ("X9", ta.int32),
                        ("X10", ta.int32)]
        self.pca_traindata = "pcadata.csv"
        self.frame1 = frame_utils.build_frame(
            self.pca_traindata, self.schema1, self.prefix)

    def test_pca_default(self):
        """Test default no. of k"""
        pca_train_out = self.frame1.daal_pca(
            ["X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9", "X10"])

        self.assertEqual(len(pca_train_out['eigen_vectors']), 10)
if __name__ == '__main__':
    unittest.main()
