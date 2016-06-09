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
    Usage:  python2.7 model_1st_class_test.py
    Test models as first-class objects.
"""
# Validate that models are first-class objects.
# Demonstrate that they can access methods and attributes previously
#   available only under Titan ML.
# Create models with
#   k_means
#   SVM
#   LibSVM
# Invoke methods
#   predict
#   test
#   train
#   rename
# Get/Set attributes
#   name

__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart"]
__version__ = "2015.01.22"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import requests

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class FirstClassModelTest(atk_test.ATKTestCase):
    """Test fixture for the logistic regression tests"""

    def setUp(self):
        super(FirstClassModelTest, self).setUp()

        dataset = "LogReg_all_0.csv"
        dataset_train = "LogReg_train_pos_neg.csv"
        schema = [("data", ia.int32), ("label", ia.int32)]

        self.frame0 = frame_utils.build_frame(dataset, schema, self.prefix)
        self.frame_pn = frame_utils.build_frame(
            dataset_train, schema, self.prefix)

    def model_smoke(self, model, name):
        """
        Run the model object through basic methods;
        :param model:   test target
        :param name:    name at creation (expected result)
        :return: None
        :result: model is dropped
        """
        # Fetch model name
        # Rename model
        # Fetch model by new name
        # Drop model; verify deletion

        # Fetch model name
        model_name = model.name
        self.assertEqual(model_name, name)

        # Rename model
        new_name = common_utils.get_a_name("major_general")
        model.name = new_name
        self.assertEqual(model.name, new_name)

        # Fetch model by new name
        new_model = ia.get_model(new_name)
        self.assertEqual(model, new_model)

        # ERROR: train model on empty column name
        self.assertRaises((requests.exceptions.HTTPError,
                           ia.rest.command.CommandServerError),
                          model.train, self.frame0, "data", "")

        # ERROR: test model on empty column name
        if not isinstance(model, ia.core.api.KMeansModel):
            self.assertRaises((requests.exceptions.HTTPError,
                               ia.rest.command.CommandServerError),
                              model.test, self.frame0, "", "label")

        # Drop model; verify deletion
        ia.drop_models(new_name)
        self.assertNotIn(new_name, ia.get_model_names())

    def test_smoke_all_model_types(self):
        """ Smoke test on all model types """
        # Logistic regression
        # k_means
        # Support vector

        sv_model_name = common_utils.get_a_name("SVM_Model")
        sv_model = ia.SvmModel(sv_model_name)
        self.model_smoke(sv_model, sv_model_name)

        svm_model_name = common_utils.get_a_name("LibSVM_Model")
        svm_model = ia.LibsvmModel(svm_model_name)
        self.model_smoke(svm_model, svm_model_name)

        km_model_name = common_utils.get_a_name("KM_Model")
        km_model = ia.KMeansModel(km_model_name)
        self.model_smoke(km_model, km_model_name)

    def test_model_method_kmm(self):
        """ Test model-based methods on k_means model """
        km_model_name = common_utils.get_a_name("KM_Model")
        km_model = ia.KMeansModel(km_model_name)
        print type(km_model)
        km_model.train(self.frame_pn, "data", 1.0)

        print self.frame0.inspect()
        predicted_frame = km_model.predict(self.frame0, 'data')
        if predicted_frame == {}:
            print "https://jira01.devtools.intel.com/browse/TRIB-4513"
        # print self.frame0.inspect()
        # print self.frame_pn.inspect()
        # print predicted_frame.inspect()
        ia.drop_models(km_model)

    def test_model_method_svm(self):
        """ Test model-based methods on support vector model """
        svm_model_name = common_utils.get_a_name("SVM_Model")
        svm_model = ia.SvmModel(svm_model_name)
        svm_model.train(self.frame0, "label", ["data"])
        train_obj = svm_model.test(self.frame0, "data", "label")
        print train_obj
        predicted_frame = svm_model.predict(self.frame0, 'data')
        if predicted_frame == {}:
            print "https://jira01.devtools.intel.com/browse/TRIB-4513"
        # print predicted_frame.inspect()
        ia.drop_models(svm_model)

    def test_model_method_libsvm(self):
        """ Test model-based methods on support vector model """
        libsvm_model_name = common_utils.get_a_name("LibSVM_Model")
        libsvm_model = ia.LibsvmModel(libsvm_model_name)
        libsvm_model.train(self.frame0, "label", ["data"])
        train_obj = libsvm_model.test(self.frame0, "data", "label")
        print train_obj
        predicted_frame = libsvm_model.predict(self.frame0, 'data')
        if predicted_frame == {}:
            print "https://jira01.devtools.intel.com/browse/TRIB-4513"
        # print predicted_frame.inspect()
        ia.drop_models(libsvm_model)

    def test_libsvm_publish(self):
        """ Test publish model """
        libsvm_model_name = common_utils.get_a_name("LibSVM_Model")
        libsvm_model = ia.LibsvmModel(libsvm_model_name)
        libsvm_model.train(self.frame0, "label", ["data"])
        train_obj = libsvm_model.test(self.frame0, "data", "label")
        path = libsvm_model.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)


if __name__ == '__main__':
    unittest.main()
