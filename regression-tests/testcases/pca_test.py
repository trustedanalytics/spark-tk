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


class PrincipalComponent(atk_test.ATKTestCase):
    # expected singular values
    expected_singular_val = [3373.70412657, 594.11385671,
                             588.713470217, 584.157023124,
                             579.433395835, 576.659495077,
                             572.267630461, 568.224352464,
                             567.328732759, 560.882281619]
    # expected right-singular vectors V
    expected_R_singular_vec = \
        [[0.315533916, -0.3942771, 0.258362247, -0.0738539198,
          -0.460673735, 0.0643077298, -0.0837131184, 0.0257963888,
          0.00376728499, 0.669876972],
         [0.316500921, -0.165508013, -0.131017612, 0.581988787,
          -0.0863507191, 0.160473134, 0.53134635, 0.41199152,
          0.0823770991, -0.156517367],
         [0.316777341, 0.244415549, 0.332413311, -0.377379981,
          0.149653873, 0.0606339992, -0.163748261, 0.699502817,
          -0.171189721, -0.124509149],
         [0.318988109, -0.171520719, -0.250278714, 0.335635209,
          0.580901954, 0.160427725, -0.531610364, -0.0304943121,
          -0.0785743304, 0.201591811],
         [0.3160833, 0.000386702461, -0.108022985, 0.167086405,
          -0.470855879, -0.256296677, -0.318727111, -0.155621638,
          -0.521547782, -0.418681224],
         [0.316721742, 0.288319245, 0.499514144, 0.267566455,
          -0.0338341451, -0.134086469, -0.184724393, -0.246523528,
          0.593753078, -0.169969303],
         [0.315335647, -0.258529064, 0.374780341, -0.169762381,
          0.416093803, -0.118232778, 0.445019707, -0.395962728,
          -0.337229123, -0.0937071881],
         [0.314899154, -0.0294147958, -0.447870311, -0.258339192,
          0.0794841625, -0.71141762, 0.110951688, 0.102784186,
          0.292018251, 0.109836478],
         [0.315542865, -0.236497774, -0.289051199, -0.452795684,
          -0.12175352, 0.5265342, -0.0312645934, -0.180142504,
          0.318334436, -0.359303747],
         [0.315875856, 0.72196434, -0.239088332, -0.0259999274,
          -0.0579153559, 0.244335633, 0.232808362, -0.233600306,
          -0.181191102, 0.3413174]]

    def setUp(self):
        super(PrincipalComponent, self).setUp()
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

    def test_pca_train_mean(self):
        """Test the train functionality with mean centering"""
        pcamodel = ta.PrincipalComponentsModel()
        pca_train_out = pcamodel.train(self.frame1,
                                       ["X1", "X2", "X3", "X4", "X5",
                                        "X6", "X7", "X8", "X9", "X10"],
                                       True, 10)

        # actual right-singular vectors
        actual_R_singular_vec = pca_train_out['right_singular_vectors']

        # actual singular values
        actual_singular_val = pca_train_out['singular_values']
        for c in self.frame1.column_names:
            mean = self.frame1.column_summary_statistics(c)["mean"]
            self.frame1.add_columns(
                lambda x: x[c] - mean, (c+"_n", ta.float32))

        pcamodelmean = ta.PrincipalComponentsModel()
        pcamodelmean.train(
            self.frame1,
            ["X1_n", "X2_n", "X3_n", "X4_n", "X5_n",
             "X6_n", "X7_n", "X8_n", "X9_n", "X10_n"],
            False, 10)

        # actual right-singular vectors
        actual_R_singular_vec_mean = pca_train_out['right_singular_vectors']
        # actual singular values
        actual_singular_val_mean = pca_train_out['singular_values']

        expected_actual = zip(actual_singular_val, actual_singular_val_mean)
        for expected, actual in expected_actual:
            self.assertAlmostEqual(expected, actual, 8)

        expected_actual = zip(
            actual_R_singular_vec, actual_R_singular_vec_mean)
        for expected, actual in expected_actual:
            for f1, f2 in zip(expected, actual):
                self.assertAlmostEqual(f1, f2, 4)

    def test_pca_predict(self):
        """Test the train functionality"""
        pcamodel = ta.PrincipalComponentsModel()
        pca_train_out = pcamodel.train(self.frame1,
                                       ["X1", "X2", "X3", "X4", "X5",
                                        "X6", "X7", "X8", "X9", "X10"],
                                       False, 10)

        predicted_frame = pcamodel.predict(self.frame1, False)
        pd_frame = predicted_frame.download(predicted_frame.row_count)
        actual_R_singular_vec = map(list, zip(*pca_train_out['right_singular_vectors']))
        for _, i in pd_frame.iterrows():
            vec1 = i[0:10]
            vec2 = i[10:]
            dot_product = [sum([(r1)*(r2) for r1, r2 in zip(vec1, k)]) for k in actual_R_singular_vec]
            for i, j in zip(vec2, dot_product):
                self.assertAlmostEqual(i, j)


    def test_pca_train(self):
        """Test the train functionality"""
        pcamodel = ta.PrincipalComponentsModel()
        pca_train_out = pcamodel.train(self.frame1,
                                       ["X1", "X2", "X3", "X4", "X5",
                                        "X6", "X7", "X8", "X9", "X10"],
                                       False, 10)

        # actual right-singular vectors
        actual_R_singular_vec = pca_train_out['right_singular_vectors']

        # actual singular values
        actual_singular_val = pca_train_out['singular_values']

        expected_actual = zip(self.expected_singular_val, actual_singular_val)
        for expected, actual in expected_actual:
            self.assertAlmostEqual(expected, actual, 8)

        expected_actual = zip(actual_R_singular_vec,
                              self.expected_R_singular_vec)
        for expected, actual in expected_actual:
            for f1, f2 in zip(expected, actual):
                self.assertAlmostEqual(abs(f1), abs(f2), 4)

    def test_pca_publish(self):
        """Test the publish functionality"""
        pcamodel = ta.PrincipalComponentsModel()
        pca_train_out = pcamodel.train(self.frame1,
                                       ["X1", "X2", "X3", "X4", "X5",
                                        "X6", "X7", "X8", "X9", "X10"],
                                       False, 10)
        path = pcamodel.publish()
        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_pca_default(self):
        """Test default no. of k"""
        pcamodel = ta.PrincipalComponentsModel()
        pca_train_out = pcamodel.train(
            self.frame1,
            ["X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9", "X10"],
            False)
        # actual right-singular vectors
        actual_R_singular_vec = pca_train_out['right_singular_vectors']

        # actual singular values
        actual_singular_val = pca_train_out['singular_values']

        for ind in xrange(0, len(actual_singular_val)):
                self.assertAlmostEqual(round(actual_singular_val[ind], 8),
                                       self.expected_singular_val[ind])

        for ind in xrange(0, len(actual_R_singular_vec)):
            for ind2 in xrange(0, len(actual_R_singular_vec[ind])):
                self.assertEqual(
                    abs(round(actual_R_singular_vec[ind][ind2], 6)),
                    abs(round(self.expected_R_singular_vec[ind][ind2], 6)))

    def test_pca_bad_no_of_k(self):
        """Test invalid k value in train"""
        pcamodel = ta.PrincipalComponentsModel()
        with self.assertRaises(ta.rest.command.CommandServerError):
                pcamodel.train(self.frame1,
                               ["X1", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               11)

    def test_pca_invalid_k(self):
        """Test k < 1 in train"""
        pcamodel = ta.PrincipalComponentsModel()
        with self.assertRaises(ta.rest.command.CommandServerError):
                pcamodel.train(self.frame1,
                               ["X1", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               0)

    def test_pca_bad_column_name(self):
        """Test bad feature column name"""
        pcamodel = ta.PrincipalComponentsModel()
        with self.assertRaises(ta.rest.command.CommandServerError):
                pcamodel.train(self.frame1,
                               ["ERR", "X2", "X3", "X4", "X5",
                                "X6", "X7", "X8", "X9", "X10"],
                               10)

    def test_pca_bad_column_type(self):
        """Test bad feature column name type"""
        pcamodel = ta.PrincipalComponentsModel()
        with self.assertRaises(ta.rest.command.CommandServerError):
                pcamodel.train(self.frame1, 10, 10)

if __name__ == '__main__':
    unittest.main()
