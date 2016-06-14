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
Usage: python2.7 SVM_2D_Slope1_test.py
Verifies SVM by running train, test and predict on two datasets which are
both in the +X, +Y quadrant and linearly separable by a hyperplane with a
slope of 1 passing through the origin point of the graph.
"""

__author__ = 'gtchurc'
__version__ = '2015-03-23A'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import atk_test
from qalib import frame_utils


class Svm2DSlope1(atk_test.ATKTestCase):

    def setUp(self):
        """Build the frame needed for these tests
        and then create and train the model."""

        super(Svm2DSlope1, self).setUp()

        self.sch2 = [("Class", int),            # Class is either 1 or 0.
                     ("Dim_1", ia.float64),     # X coordinate on graph
                     ("Dim_2", ia.float64)]     # Y coordinate on graph
        self.train_file = "SVM-2F-train-50X50_1SlopePlus0.csv"
        self.trainer = frame_utils.build_frame(
            self.train_file, self.sch2, self.prefix)

        self.model = ia.SvmModel()
        self.model.train(self.trainer, "Class", ["Dim_1", "Dim_2"])

    def test_svm_model_test(self):
        """Perform the test against a dataset with the same
        hyperplane dividing the sets."""
        test_file = "SVM-2F-test-50X50_1SlopePlus0.csv"
        test_frame = frame_utils.build_frame(test_file, self.sch2, self.prefix)
        results = self.model.test(test_frame, "Class")
        # As this special case returns 100% results all of the stats come
        # out as 1.0 rather than some fractional value.
        expected_stat = 1.0
        self.assertEqual(expected_stat,
                         results.recall,
                         "Recall: %f didn't match %f" %
                         (results.recall, expected_stat))

        self.assertEqual(expected_stat,
                         results.accuracy,
                         "Recall: %f didn't match %f" %
                         (results.accuracy, expected_stat))

        self.assertEqual(expected_stat,
                         results.precision,
                         "Recall: %f didn't match %f" %
                         (results.precision, expected_stat))

        self.assertEqual(expected_stat,
                         results.f_measure,
                         "Recall: %f didn't match %f" %
                         (results.f_measure, expected_stat))

        # Now we verify the confusion matrix contains the expected results.
        cf = results.confusion_matrix
        self.assertEqual(cf['Predicted_Pos']['Actual_Pos'], 95,
                         "Actual positive was not expected value!")
        self.assertEqual(cf['Predicted_Neg']['Actual_Pos'], 0,
                         "False negative didn't match expected value.")
        self.assertEqual(cf['Predicted_Pos']['Actual_Neg'], 0,
                         "False positive was not expected value!")
        self.assertEqual(cf['Predicted_Neg']['Actual_Neg'], 105,
                         "Actual negative didn't match expected value.")
        # Uncomment the line below to display the contents of the
        #   confusion matrix.
        # print confusion_mat

    def test_svm_model_predict(self):
        """Test the predict function on the cakewalk
        training and testing datasets."""
        val_data = "SVM-2F-test-50X50_1SlopePlus0.csv"
        val_frame = frame_utils.build_frame(val_data, self.sch2, self.prefix)
        validation = self.model.predict(val_frame)

        # Verify that values in 'predict' and 'Class' columns match.
        outcome = validation.take(validation.row_count)
        for row in outcome:
            self.assertEqual(row[0],    # Compare the class column with the
                             row[3],    # predicted value provided.
                             "Class doesn't match predict for %s" % row)

if __name__ == "__main__":
    unittest.main()
