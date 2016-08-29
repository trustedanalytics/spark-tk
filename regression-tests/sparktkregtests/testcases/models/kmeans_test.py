"""Calculate kmeans against known dataset with known centroids """
import unittest
from sparktkregtests.lib import sparktk_test


class KMeansClustering(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to test against."""
        super(KMeansClustering, self).setUp()
        schema = [("Vec1", float),
                  ("Vec2", float),
                  ("Vec3", float),
                  ("Vec4", float),
                  ("Vec5", float),
                  ("term", str)]

        self.frame_train = self.context.frame.import_csv(
            self.get_file("kmeans_train.csv"), schema=schema)
        self.frame_test = self.context.frame.import_csv(
            self.get_file("kmeans_test.csv"), schema=schema)

    def test_different_columns(self):
        """Tests kmeans cluster algorithm with more iterations."""
        result = self.context.models.clustering.kmeans.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            scalings=[1.0, 1.0, 1.0, 1.0, 1.0], k=5, max_iter=300)

        self.assertAlmostEqual(
            83379.0, result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1, 6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        self.frame_test.rename_columns(
            {"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3",
             "Vec4": "Dim4", "Vec5": 'Dim5'})
        kmodel.predict(
            self.frame_test, ['Dim1', 'Dim2', 'Dim3', 'Dim4', 'Dim5'])
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)

    def test_kmeans_standard(self):
        """Tests standard usage of the kmeans cluster algorithm."""
        result = self.context.models.clustering.kmeans.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            scalings=[1.0, 1.0, 1.0, 1.0, 1.0], k=5)
        print "test kmeans standard: " + str(result)
        self._validate(result, self.frame_train)

    def test_column_weights(self):
        """Tests kmeans cluster algorithm with weighted values."""
        result = self.context.models.clustering.kmeans.train(self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], scalings=[0.1, 0.1, 0.1, 0.1, 0.1], k=5)
        self._validate(result, kmodel, 834.0)

    def test_max_iterations(self):
        """Tests kmeans cluster algorithm with more iterations."""
        result = self.context.models.clustering.kmeans.train(self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], scalings=[1.0, 1.0, 1.0, 1.0, 1.0], k=5, max_iter=35)
        self._validate(result, kmodel)

    def test_epsilon_assign(self):
        """Tests kmeans cluster algorithm with an arbitrary epsilon. """
        result = self.context.models.clustering.kmeans.train(self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], scalings=[1.0, 1.0, 1.0, 1.0, 1.0], k=5, epsilon=.000000000001)
        self._validate(result, kmodel)

    @unittest.skip("publish model does not yet exist")
    def test_publish(self):
        """Tests kmeans cluster publish."""
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5, initialization_mode="random")
        path = kmodel.publish()

        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_max_iterations_negative(self):
        """Check error on negative number of iterations."""
        with self.assertRaisesRegexp(Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.kmeans.train(self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=5, max_iter=-3)

    def test_max_iterations_bad_type(self):
        """Check error on a floating point number of iterations."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"], scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=5, max_iter=[])

    def test_k_negative(self):
        """Check error on negative number of clusters."""
        with self.assertRaisesRegexp(Exception, "k must be at least 1"):
            self.context.models.clustering.kmeans.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=-5)

    def test_k_bad_type(self):
        """Check error on float number of clusters."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=[])

    def test_epsilon_negative(self):
        """Check error on negative epsilon value."""
        with self.assertRaisesRegexp(Exception, "epsilon must be a positive value"):
            self.context.models.clustering.kmeans.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=5, epsilon=-0.05)

    def test_epsilon_bad_type(self):
        """Check error on bad epsilon type."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=5, epsilon=[])

    @unittest.skip("Invalid/too few columns for model.predict should give a useful exception")
    def test_invalid_columns_predict(self):
        """Check error on a floating point number of iterations."""
        with self.assertRaisesRegexp(Exception, "foo"):
            kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                         k=5, max_iter=[])
            self.frame_test.rename_columns(
                {"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3",
                 "Vec4": "Dim4", "Vec5": 'Dim5'})
            kmodel.predict(self.frame_test)

    @unittest.skip("Invalid/too few columns for model.predict should give a useful exception")
    def test_too_few_columns(self):
        """Check error on a floating point number of iterations."""
        with self.assertRaisesRegexp(Exception, "foo"):
            kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                         k=5, max_iter=[])
            kmodel.predict(self.frame_test, columns=["Vec1", "Vec2"])

    @unittest.skip("Model training with null frame should give a useful exception message")
    def test_null_frame(self):
        """Check error on null frame."""
        with self.assertRaisesRegexp(Exception, "foo"):
            self.context.models.clustering.kmeans.train(None,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         scalings=[0.01, 0.01, 0.01, 0.01, 0.01], k=5)

    def _validate(self, result, kmodel, val=83379.0):
        self.assertAlmostEqual(
            val, result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1, 6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        kmodel.predict(self.frame_test)
        test_take = self.frame_test.download()
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)


if __name__ == '__main__':
    unittest.main()
