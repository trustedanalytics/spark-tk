"""Test guassian mixture models against known values"""
import unittest
import random
from collections import Counter
from numpy import array
from sparktkregtests.lib import sparktk_test
from sklearn.datasets.samples_generator import make_blobs


class GMMModelTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        #data_file = self.get_file("gmm_data.csv")
        x,y = make_blobs(n_samples=50, centers=5, n_features=2, random_state=14)
        self.data = x.tolist()
        self.frame = self.context.frame.create(
            self.data, schema=[("x1", float), ("x2", float)])
        
    def test_train(self):
        """ Verify that model operates as expected in straightforward case"""
        model = self.context.models.clustering.gmm.train(
                    self.frame, ["x1", "x2"],
                    column_scalings=[1.0, 1.0],
                    k=5,
                    max_iterations=500,
                    seed=20,
                    convergence_tol=0.0001)
        print model.gaussians
        #TO-DO: check mu and sigma when bug is fixed
        """
        [MultivariateGaussian(mu=DenseVector([6.99, -10.4285]), sigma=DenseMatrix(2, 2, [0.1817, -0.0555, -0.0555, 0.3378], 0)),
         MultivariateGaussian(mu=DenseVector([1.0314, -1.3319]), sigma=DenseMatrix(2, 2, [1.1462, -5.2027, -5.2027, 24.0738], 0)),
         MultivariateGaussian(mu=DenseVector([8.9029, -9.7618]), sigma=DenseMatrix(2, 2, [0.0226, 0.0162, 0.0162, 1.0214], 0)),
         MultivariateGaussian(mu=DenseVector([-2.1822, 7.5628]), sigma=DenseMatrix(2, 2, [5.7653, -2.427, -2.427, 2.3096], 0)),
         MultivariateGaussian(mu=DenseVector([0.156, -5.174]), sigma=DenseMatrix(2, 2, [0.3253, -0.4275, -0.4275, 2.2137], 0))]
        """

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

        data = map(tuple, self.data)
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
