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
""" publish a model of each type
    usage: python2.7 publish_model_test.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ta

from qalib import common_utils
from qalib import frame_utils
from qalib import hdfs_utils
from qalib import atk_test


class PublishModel(atk_test.ATKTestCase):

    @classmethod
    def setUpClass(cls):
        super(PublishModel, cls).setUpClass()
        cls.hdfs_conn = hdfs_utils.HDFSConnection(ta.server.host,
                                                  proxy_user="atkuser",
                                                  username="hadoop")

    def setUp(self):
        """Match model names with types"""
        super(PublishModel, self).setUp()

        self.model_dict = {
            # Type     Model name                           Is this type publishable?
            'KMeans': 'KMeans_PubModel',                        # yes, has a publish method
            # 'Linear Reg': 'LinReg_PubModel',                  # no publish method
            # 'Logistic Reg': 'LogReg_PubModel',                # no
            'LDA': 'LDA_PubModel',                              # yes
            'Naive Bayes': 'NBayes_PubModel',                   # yes
            'Prin Comp': 'PCA_PubModel',                        # yes
            'Random Forest Reg': 'Forest_Reg_PubModel',         # yes
            'Random Forest Class': 'Forest_Class_PubModel',     # yes
            'SVM': 'SVM_PubModel',                              # yes
            'LibSVM': 'Libsvm_PubModel',                        # yes
            # 'Collab Filter': 'Collab_PubModel',               # no
            # 'Daal': 'Daal_PubModel',                          # no
            # 'Power Iter': 'PIC_PubModel',                     # no
        }

        self.dataset = "LogReg_all_0.csv"
        self.dataset_train = "LogReg_train_pos_neg.csv"

        self.schema = [("data", ta.int32), ("label", ta.int32)]

    def test_refresh_all_models(self):

        model_roster = ta.get_model_names()
        print model_roster

        # 'Collab Filter': 'Collab_PubModel'

        # 'KMeans': 'KMeans_PubModel'
        model_name = "KMeans_PubModel"
        if model_name not in model_roster:
            print "Rebuild KMeans model"
            frame_kmeans = frame_utils.build_frame(
                self.dataset_train, self.schema)
            km_model = ta.KMeansModel(model_name)
            print type(km_model)
            km_model.train(frame_kmeans, "data", 1.0)
            print ta.get_model_names()

        # 'SVM': 'SVM_PubModel'
        frame_svm = None
        model_name = "SVM_PubModel"
        if model_name not in model_roster:
            print "Rebuild ML SVM model"
            frame_svm = frame_utils.build_frame(self.dataset, self.schema)
            svm_model = ta.SvmModel(model_name)
            svm_model.train(frame_svm, "label", ["data"])
            svm_model.test(frame_svm, "data", "label")
            print ta.get_model_names()

        # 'LibSVM': 'Libsvm_PubModel'
        model_name = "Libsvm_PubModel"
        if model_name not in model_roster:
            print "Rebuild LibSVM model"
            if frame_svm is None:
                frame_svm = frame_utils.build_frame(self.dataset, self.schema)
            libsvm_model = ta.LibsvmModel(model_name)
            libsvm_model.train(frame_svm, "label", ["data"])
            libsvm_model.test(frame_svm, "data", "label")
            print ta.get_model_names()

        # 'Linear Reg': 'LinReg_PubModel'

        # 'Logistic Reg': 'LogReg_PubModel'

        # 'LDA': 'LDA_PubModel'
        model_name = "LDA_PubModel"
        if model_name not in model_roster:
            print "Rebuild LDA model"
            schema = [('paper', str),
                      ('word', str),
                      ('count', ta.int64),
                      ('topic', str)]
            frame_lda = frame_utils.build_frame(
                "lda8.csv", schema, self.prefix)
            lda_model = ta.LdaModel(model_name)
            lda_model.train(frame_lda, 'paper', 'word', 'count',
                            num_topics=5, max_iterations=60)
            print ta.get_model_names()

        # 'Naive Bayes': 'NBayes_PubModel'

        # 'Prin Comp': 'PCA_PubModel'
        model_name = "PCA_PubModel"
        if model_name not in model_roster:
            print "Rebuild PCA model"
            schema = [("X1", ta.int32),
                      ("X2", ta.int32),
                      ("X3", ta.int32),
                      ("X4", ta.int32),
                      ("X5", ta.int32),
                      ("X6", ta.int32),
                      ("X7", ta.int32),
                      ("X8", ta.int32),
                      ("X9", ta.int32),
                      ("X10", ta.int32)]
            pca_traindata = "pcadata.csv"
            frame_pca = frame_utils.build_frame(pca_traindata, schema)
            pca_model = ta.PrincipalComponentsModel(model_name)
            pca_model.train(frame_pca, frame_pca.column_names, False, 10)
            print ta.get_model_names()

        # 'Random Forest Reg': 'Forest_Reg_PubModel'
        rf_frame = None
        model_name = "Forest_Reg_PubModel"
        if model_name not in model_roster:
            print "Rebuild RF Regression model"
            schema = [("feat1", ta.int32), ("feat2", ta.int32), ("class", str)]
            filename = "rand_forest_class.csv"
            rf_frame = frame_utils.build_frame(filename, schema, self.prefix)

            rfreg_model = ta.RandomForestClassifierModel(model_name)
            rfreg_model.train(rf_frame, "class", ["feat1", "feat2"], seed=0)
            print ta.get_model_names()

        # 'Random Forest Class': 'Forest_Class_PubModel'
        model_name = "Forest_Class_PubModel"
        if model_name not in model_roster:
            print "Rebuild RF Classifier model"
            if rf_frame is None:
                schema = [("feat1", ta.int32),
                          ("feat2", ta.int32),
                          ("class", str)]
                filename = "rand_forest_class.csv"
                rf_frame = frame_utils.build_frame(
                    filename, schema, self.prefix)

            rfclass_model = ta.RandomForestRegressorModel(model_name)
            rfclass_model.train(rf_frame, "class", ["feat1", "feat2"], seed=0)
            print ta.get_model_names()

        # 'Collab Filter': 'Collab_PubModel'    # no
        # 'Daal': 'Daal_PubModel',     # no
        # 'Power Iter': 'PIC_PubModel',     # no


        print ta.get_model_names()

    def test_model_publish(self):
        """Publish an existing model of each supported type."""

        for model_type, model_name in self.model_dict.items():

            print "\n----------------\n", model_type, model_name
            model = ta.get_model(model_name)
            model_uri = model.publish()
            print "Publish", model_type, "model as", model_uri

            # Validate reasonable-looking file name
            self.assertEqual("hdfs://", model_uri[:7])
            self.assertEqual(".tar", model_uri[-4:])
            self.assertIn("atkuser", model_uri)
            self.assertIn("model", model_uri)

            # Extract HDFS name and remove the installation.
            pos = model_uri.find('/user')
            model_name = model_uri[pos:]
            print "Local name of model:", model_name
            self.hdfs_conn.remove(model_name)

    def test_untrained_model(self):
        """Publish on an untrained model should throw an exception."""
        model = ta.LibsvmModel(common_utils.get_a_name("empty_model"))
        self.assertRaises(ta.rest.command.CommandServerError, model.publish)


if __name__ == '__main__':
    unittest.main()
