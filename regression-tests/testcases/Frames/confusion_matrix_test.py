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

from sparktk import TkContext
from qalib import atk_test, common_utils

class ConfusionMatrix(atk_test.ATKTestCase):

    def test_confusion_matrix_for_data_33_55(self):
        """Verify the input and baselines exist before running the tests."""
        super(ConfusionMatrix, self).setUp()
        self.schema1 = [("user_id", int),
                        ("vertex_type", str),
                        ("movie_id", int),
                        ("rating", int),
                        ("splits", str),
                        ("predicted", int)]
        self.model = "model_33_50.csv" # our data file in qa_data
	self.filepath = common_utils.get_file(self.model)
        self.actual_result = [6534, 3266, 6535, 3267] # what we expect to get from the confusion matrix
	tc = self.tk_context # getting a sparktk context
	#self.csv_path = "qa_data/" + self.model # path to the csv file with our data
	frame = tc.frame.import_csv(str(self.filepath), schema=self.schema1) # imports our data and returns a frame
	classMetrics = frame.binary_classification_metrics('rating', 'predicted', 1) # params are label column, result column, pos column
	confMatrix = classMetrics.confusion_matrix.values 
	cumulative_matrix_list = [confMatrix[0][0], confMatrix[0][1], confMatrix[1][0], confMatrix[1][1]]
	self.assertEqual(self.actual_result, cumulative_matrix_list) # compare our confusion matrix values with the expected values


if __name__ == '__main__':
    unittest.main()
