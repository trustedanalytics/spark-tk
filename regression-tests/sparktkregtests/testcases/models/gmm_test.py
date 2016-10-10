# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""Test guassian mixture models against known values"""
import unittest
from numpy.testing import assert_almost_equal
from sparktkregtests.lib import sparktk_test


class GMMModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        data_file = self.get_file("gmm_data.csv")
        self.frame = self.context.frame.import_csv(
            data_file, schema=[("x1", float), ("x2", float)])

    def test_train(self):
        """ Verify that model operates as expected in straightforward case"""
        model = self.context.models.clustering.gmm.train(
                    self.frame, ["x1", "x2"],
                    column_scalings=[1.0, 1.0],
                    k=5,
                    max_iterations=500,
                    seed=20,
                    convergence_tol=0.0001)

        actual_mu = [g.mu for g in model.gaussians]
        actual_sigma = [g.sigma for g in model.gaussians]
        expected_mu = \
            [[7.0206, -10.1706],
            [7.8322, -10.2383],
            [-1.3816, 6.7215],
            [-0.04184, 5.8039],
            [-4.1743, 8.5564]]
        expected_sigma = \
            [[[0.2471, -0.3325],
            [-0.3325, 0.5828]],
            [[2.3005, 0.6906],
            [0.6906, 2.1103]],
            [[1.5941, -3.5325],
            [-3.5325, 7.8424]],
            [[0.9849, 0.04328],
            [0.04328, 0.3736]],
            [[0.1168, 0.1489],
            [0.1489, 0.9757]]]
        assert_almost_equal(actual_mu, expected_mu, decimal=3)
        assert_almost_equal(actual_sigma, expected_sigma, decimal=3)

    def test_predict(self):
        """ Tests output of predict """
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"],
            column_scalings=[1.0, 1.0],
            k=3,
            max_iterations=100,
            seed=15)
        model.predict(self.frame)
        results_df = self.frame.to_pandas(self.frame.count())

        actual_cluster_sizes = Counter(results_df["predicted_cluster"].tolist())
        expected_cluster_sizes = {2: 27, 0: 17, 1: 6}
        self.assertItemsEqual(actual_cluster_sizes, expected_cluster_sizes)

    def test_gmm_1_cluster(self):
        """Test gmm doesn't error on k=1"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], [1.0, 1.0], k=1)

    def test_gmm_1_iteration(self):
        """Train on 1 iteration only, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1"], column_scalings=[1.0],
            max_iterations=1)

    def test_gmm_high_convergence(self):
        """Train on high convergence, should not throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0],
            convergence_tol=1e6)

    def test_gmm_negative_seed(self):
        """Train on negative seed, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0],
            seed=-20)

    def test_gmm_0_scalings(self):
        """all-zero column scalings, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[0.0, 0.0])

    def test_gmm_negative_scalings(self):
        """negative column scalings, shouldn't throw exception"""
        model = self.context.models.clustering.gmm.train(
            self.frame, ["x1", "x2"], column_scalings=[-1.0, -1.0])

    def test_gmm_empty_frame(self):
        """ Verify that model operates as expected in straightforward case"""
        # Train on an empty frame
        block_data = []
        frame = self.context.frame.create(
            block_data,
            [("x1", float)])

        with self.assertRaisesRegexp(
                Exception, "empty collection"):
            self.context.models.clustering.gmm.train(
                frame, ["x1"], column_scalings=[1.0])

    def test_0_classes_errors(self):
        """Train on 0 classes, should error"""
        with self.assertRaisesRegexp(
                Exception, "k must be at least 1"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0], k=0)

    def test_negative_classes(self):
        """Train on negative classes, should error"""
        with self.assertRaisesRegexp(
                Exception, "k must be at least 1"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0], k=-5)

    def test_0_iterations(self):
        """Train on 0 iterations, should error"""
        with self.assertRaisesRegexp(
                Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0],
                max_iterations=0)

    def test_negative_iterations(self):
        """Train on negative iterations, should error"""
        with self.assertRaisesRegexp(
                Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[1.0],
                max_iterations=-20)

    def test_wrong_column_scalings(self):
        """Insufficient column scalings, should error"""
        with self.assertRaisesRegexp(
                Exception, "columnWeights must not be null or empty"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1"], column_scalings=[])

    def test_too_many_column_scalings(self):
        """Extra column scalings, should error"""
        with self.assertRaisesRegexp(
                Exception,
                "Length of columnWeights and observationColumns.*"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], column_scalings=[1.0, 1.0, 1.0])

    def test_missing_column_scalings(self):
        """Missing column scalings, should error"""
        with self.assertRaisesRegexp(
            TypeError, "train\(\) takes at least 3 arguments.*"):
            self.context.models.clustering.gmm.train(
                self.frame, ["x1", "x2"], k=2)

if __name__ == "__main__":
    unittest.main()
