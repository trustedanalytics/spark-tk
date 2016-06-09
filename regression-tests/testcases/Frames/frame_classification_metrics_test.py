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
""" test cases for multinomial and count confusion matrix
    usage: python2.7 frame_classification_metrics.py

    Tests the confusion matrix functionality against known values. Confusion
    matrix is returned as a pandas frame.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

from qalib import frame_utils
from qalib import atk_test


class ClassificationMetrics(atk_test.ATKTestCase):

    def setUp(self):
        """Verify the input and baselines exist before running the tests."""
        super(ClassificationMetrics, self).setUp()
        self.schema1 = [("actual", int), ("predicted", int), ("count", int)]
        self.class_csv = "class_data.csv"
        # [true_positive, false_negative, false_positive, true_negative]
        self.actual_result = [5, 2, 3, 0, 5, 3, 0, 2, 5]

        self.frame1 = frame_utils.build_frame(
            self.class_csv, self.schema1, self.prefix)

    def test_multinomial_frame_classification(self):
        """Tests the confusion matrix functionality"""
        cm = self.frame1.classification_metrics(
            'actual', 'predicted', frequency_column='count')
        # pandas frame is returned for confusion matrix
        print cm
        conf_matrix = cm.confusion_matrix.values
        print conf_matrix
        cumulative_matrix_list = [conf_matrix[0][0],
                                  conf_matrix[0][1],
                                  conf_matrix[0][2],
                                  conf_matrix[1][0],
                                  conf_matrix[1][1],
                                  conf_matrix[1][2],
                                  conf_matrix[2][0],
                                  conf_matrix[2][1],
                                  conf_matrix[2][2]]
        self.assertEqual(self.actual_result, cumulative_matrix_list)

if __name__ == '__main__':
    unittest.main()
