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
""" test cases for the confusion matrix
    usage: python2.7 random_forest_test.py

    Tests the random forest functionality
    Both regression and classification are tested

    This testing is somewhat cursory as this code merely calls the underlying
    MLLib Library

    TODO:
        Advanced Regression Testing
        Multinomial regression

"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class RandomForest(atk_test.ATKTestCase):

    def setUp(self):
        """Build the required frame"""
        super(RandomForest, self).setUp()

        schema = [("feat1", ia.int32), ("feat2", ia.int32), ("class", str)]
        filename = "rand_forest_class.csv"

        self.frame = frame_utils.build_frame(filename, schema, self.prefix)

    def test_rand_forest_class_publish(self):
        """Test binomial classification of random forest model"""
        rfmodel = ia.RandomForestClassifierModel()
        learn_report = rfmodel.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        path = rfmodel.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_rand_forest_class(self):
        """Test binomial classification of random forest model"""
        rfmodel = ia.RandomForestClassifierModel()
        learn_report = rfmodel.train(
            self.frame, "class", ["feat1", "feat2"], seed=0)
        report_baseline = {u'impurity': u'gini',
                           u'max_bins': 100,
                           u'observation_columns': [u'feat1', u'feat2'],
                           u'num_nodes': 9,
                           u'max_depth': 4,
                           u'seed': 0,
                           u'num_trees': 1,
                           u'label_column': u'class',
                           u'feature_subset_category': u'all',
                           u'num_classes': 2}
        self.assertEqual(report_baseline, learn_report)

        predresult = rfmodel.predict(self.frame)
        preddf = predresult.download(predresult.row_count)
        for _, i in preddf.iterrows():
            self.assertEqual(i['class'], str(i['predicted_class']))

        test_res = rfmodel.test(self.frame, "class")

        self.assertEqual(test_res.precision, 1.0)
        self.assertEqual(test_res.recall, 1.0)
        self.assertEqual(test_res.accuracy, 1.0)
        self.assertEqual(test_res.f_measure, 1.0)

        self.assertEqual(
            test_res.confusion_matrix['Predicted_Pos']['Actual_Pos'], 413)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Pos']['Actual_Neg'], 0)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Neg']['Actual_Pos'], 0)
        self.assertEqual(
            test_res.confusion_matrix['Predicted_Neg']['Actual_Neg'], 587)

    def test_rand_forest_regression(self):
        """Test binomial classification of random forest model"""
        rfmodel = ia.RandomForestRegressorModel()
        rfmodel.train(self.frame, "class", ["feat1", "feat2"], seed=0)
        {u'impurity': u'variance',
         u'max_bins': 100,
         u'observation_columns': [u'feat1', u'feat2'],
         u'num_nodes': 9,
         u'max_depth': 4,
         u'seed': 0,
         u'num_trees': 1,
         u'label_column': u'class',
         u'feature_subset_category': u'all'}

        predresult = rfmodel.predict(self.frame)
        preddf = predresult.download(predresult.row_count)
        for _, i in preddf.iterrows():
            self.assertAlmostEqual(float(i['class']), i['predicted_value'])

    def test_rand_forest_publish(self):
        """Test binomial classification of random forest model"""
        rfmodel = ia.RandomForestRegressorModel()
        rfmodel.train(self.frame, "class", ["feat1", "feat2"], seed=0)
        path = rfmodel.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)


if __name__ == '__main__':
    unittest.main()
