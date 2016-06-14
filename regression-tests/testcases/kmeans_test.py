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
""" test cases for the kmeans clustering algorithm
    usage: python2.7 confusion_matrix_test.py

    A dataset generator is included
    Fixed datasets were generated, the results reaped and verified, those
    results are considered a ground truth
"""

# TODO: Get a fixed PRNG for this system.

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class KMeansClustering(atk_test.ATKTestCase):

    def setUp(self):
        """Import the files to test against."""
        super(KMeansClustering, self).setUp()
        schema = [("Vec1", ia.float64),
                  ("Vec2", ia.float64),
                  ("Vec3", ia.float64),
                  ("Vec4", ia.float64),
                  ("Vec5", ia.float64),
                  ("term", str)]

        self.frame_train = frame_utils.build_frame(
            "kmeans_train.csv", schema, self.prefix)
        self.frame_test = frame_utils.build_frame(
            "kmeans_test.csv", schema, self.prefix)

    def test_different_columns(self):
        """Tests kmeans cluster algorithm with more iterations."""
        # need to revisit data, is it not disparate enough?
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0],
                     5, max_iterations=300)

        self.assertAlmostEqual(83379.0,
            result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1,6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        self.frame_test.rename_columns({"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3", "Vec4": "Dim4", "Vec5": 'Dim5'})
        test_frame = kmodel.predict(self.frame_test, ['Dim1', 'Dim2', 'Dim3', 'Dim4', 'Dim5'])
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)

    def test_model_status(self):
        """Tests model status."""
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)
        self.assertEqual(kmodel.status, "ACTIVE")
        ia.drop_models(kmodel)
        self.assertEqual(kmodel.status, "DROPPED")

    def test_model_last_read_date(self):
        """Tests model last read date."""
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)
        old_read = kmodel.last_read_date
        kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0], 5)
        new_read = kmodel.last_read_date
        self.assertNotEqual(old_read, new_read)

    def test_kmeans_standard(self):
        """Tests standard usage of the kmeans cluster algorithm."""
        # No asserts because this test is too unstable
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0], 5)

        self.assertAlmostEqual(83379.0,
            result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1,6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        test_frame = kmodel.predict(self.frame_test)
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)


    def test_column_weights(self):
        """Tests kmeans cluster algorithm with weighted values."""
        # no asserts because test is too unstable
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [0.01, 0.01, 0.01, 0.01, 0.01], 5)

        self.assertAlmostEqual(8.0,
            result['within_set_sum_of_squared_error'], delta=1)
        for i in range(1,6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        test_frame = kmodel.predict(self.frame_test)
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)

    def test_max_iterations(self):
        """Tests kmeans cluster algorithm with more iterations."""
        # need to revisit data, is it not disparate enough?
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0],
                     5, max_iterations=300)

        self.assertAlmostEqual(83379.0,
            result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1,6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        test_frame = kmodel.predict(self.frame_test)
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)

    def test_epsilon_assign(self):
        """Tests kmeans cluster algorithm with an arbitrary epsilon. """
        # Tests too unstable, need to be able to control PRNG Seed
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [0.01, 0.01, 0.01, 0.01, 0.01],
                     5, epsilon=.000000000001)

        self.assertAlmostEqual(8.0,
            result['within_set_sum_of_squared_error'], delta=1)
        for i in range(1,6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        test_frame = kmodel.predict(self.frame_test)
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)

    def test_intialization_mode_random(self):
        """Tests kmeans cluster algorithm with random seeds."""
        # no asserts performed since this testcase is too random
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0],
                     5, initialization_mode="random")

    def test_publish(self):
        """Tests kmeans cluster algorithm with random seeds."""
        # no asserts performed since this testcase is too random
        model_name = common_utils.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0],
                     5, initialization_mode="random")
        path = kmodel.publish()

        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_max_iterations_negative(self):
        """Check error on negative number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01],
                         5, max_iterations=-3)

    def test_max_iterations_bad_type(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01],
                         5, max_iterations=[])

    def test_k_negative(self):
        """Check error on negative number of clusters."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], -5)

    def test_k_bad_type(self):
        """Check error on float number of clusters."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], [])

    def test_epsilon_negative(self):
        """Check error on negative epsilon value."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], 5, epsilon=-0.05)

    def test_epsilon_bad_type(self):
        """Check error on bad epsilon type."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], 5, epsilon=[])


    def test_initialization_mode_bad_type(self):
        """Check error on bad initialization type."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01],
                         5, initialization_mode=3)

    def test_initialization_mode_bad_value(self):
        """Check error on bad initialization value."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01],
                         5, initialization_mode="badvalue")

    def test_invalid_columns_predict(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [1.0, 1.0, 1.0, 1.0, 1.0],
                         5, max_iterations=[])
            self.frame_test.rename_columns({"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3", "Vec4": "Dim4", "Vec5": 'Dim5'})
            kmodel.predict(self.frame_test)

    def test_too_few_columns(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [1.0, 1.0, 1.0, 1.0, 1.0],
                         5, max_iterations=[])
            kmodel.predict(self.frame_test, ["Vec1", "Vec2"])

    def test_null_frame(self):
        """Check error on null frame."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = common_utils.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(None,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], 5)


if __name__ == '__main__':
    unittest.main()
