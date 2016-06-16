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
""" test cases for the confusion matrix
    usage: python2.7 confusion_matrix_test.py

    Tests the confusion matrix functionality against known values. Confusion
    matrix is returned as a pandas frame.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import unittest

import trustedanalytics as ia
#from sparktk.frame.constructors import create
from sparktk import TkContext

from qalib import common_utils as cu
from qalib import atk_test


class ConfusionMatrix(atk_test.ATKTestCase):

    def setUp(self):
        """Verify the input and baselines exist before running the tests."""
        super(ConfusionMatrix, self).setUp()
        self.schema1 = [("user_id", ia.int32),
                        ("vertex_type", str),
                        ("movie_id", ia.int32),
                        ("rating", ia.int32),
                        ("splits", str),
                        ("predicted", ia.int32)]
        #self.model_33_50 = "model_33_50.csv"
	model_33_50 = cu.get_file("model_33_50.csv")
        # [true_positive, false_negative, false_positive, true_negative]
        self.actual_result = [6534, 3266, 6535, 3267]
	print "model: " + str(model_33_50)
	tc = atk_test.ATKTestCase.get_context()
	
	model = tc.frame.import_csv(model_33_50)
	self.frame1 = tc.frame.create(model)
	#create(model_33_50, schema=self.schema1)
        #self.frame1 = frame_utils.build_frame(
        #    self.model_33_50, self.schema1, self.prefix)

    def test_confusion_matrix_for_data_33_55(self):
        """Tests the confusion matrix functionality"""
        cm = self.frame1.classification_metrics('rating', 'predicted', 1)
        # pandas frame is returned for confusion matrix
        conf_matrix = cm.confusion_matrix.values
        cumulative_matrix_list = [conf_matrix[0][0],
                                  conf_matrix[0][1],
                                  conf_matrix[1][0],
                                  conf_matrix[1][1]]
        self.assertEqual(self.actual_result, cumulative_matrix_list)

if __name__ == '__main__':
    unittest.main()
