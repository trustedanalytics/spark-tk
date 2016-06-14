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
 Tests Naive Bayes Model against known values.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class NaiveBayes(atk_test.ATKTestCase):

    def setUp(self):
        """Build the frames needed for the tests."""
        super(NaiveBayes, self).setUp()

        dataset = "naive_bayes_data.txt"
        schema = [("label", ia.int32),
                  ("f1", ia.int32),
                  ("f2", ia.int32),
                  ("f3", ia.int32)]
        self.frame = frame_utils.build_frame(dataset, schema, self.prefix)

    def test_model_train_empty_feature(self):
        """Test empty string for training features throws errors."""
        model = ia.NaiveBayesModel()
        with(self.assertRaises((RuntimeError,
                                ia.rest.command.CommandServerError))):
            model.train(self.frame, "label", "")

    def test_model_train_empty_label_coloum(self):
        """Test empty string for label coloum throws error."""
        model = ia.NaiveBayesModel()
        with(self.assertRaises((RuntimeError,
                                ia.rest.command.CommandServerError))):
            model.train(self.frame, "", "['f1', 'f2', 'f3']")

    def test_model_test(self):
        """Test training intializes theta, pi and labels"""
        model = ia.NaiveBayesModel()
        model.train(self.frame, "label", ['f1', 'f2', 'f3'])

        # This is hacky, should really train on another
        # dataset
        res = model.test(self.frame, "label", ['f1', 'f2', 'f3'])
        self.assertAlmostEqual(res.recall, 1.0)
        self.assertAlmostEqual(res.precision, 1.0)
        self.assertAlmostEqual(res.f_measure, 1.0)
        self.assertAlmostEqual(res.accuracy, 1.0)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Pos"]["Actual_Pos"], 2)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Pos"]["Actual_Neg"], 0)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Neg"]["Actual_Neg"], 4)
        self.assertAlmostEqual(
            res.confusion_matrix["Predicted_Neg"]["Actual_Pos"], 0)

    def test_model_publish_bayes(self):
        """Test training intializes theta, pi and labels"""
        model = ia.NaiveBayesModel()
        model.train(self.frame, "label", ['f1', 'f2', 'f3'])
        path = model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_model_test_paramater_initiation(self):
        """Test training intializes theta, pi and labels"""
        model = ia.NaiveBayesModel()
        model.train(self.frame, "label", ['f1', 'f2', 'f3'])

        # This is hacky, should really train on another
        # dataset
        res = model.predict(self.frame, ['f1', 'f2', 'f3'])
        analysis = res.download()
        for (ix, i) in analysis.iterrows():
            self.assertEqual(i["predicted_class"], i["label"])

    def test_get_model_names(self):
        """Test getting model names."""
        name = common_utils.get_a_name(self.prefix)
        ia.NaiveBayesModel(name)
        self.assertTrue(name in ia.get_model_names())

    def test_get_model(self):
        """Test getting model by name."""
        name = common_utils.get_a_name(self.prefix)
        model = ia.NaiveBayesModel(name)
        model_get = ia.get_model(name)
        self.assertEqual(model_get.name, model.name)

if __name__ == '__main__':
    unittest.main()
