"""Calculate kmeans against known dataset with known centroids """
import unittest
from qalib import sparktk_test


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
        kmodel = self.context.models.clustering.kmeans.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5, max_iterations=300)

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

    def test_model_status(self):
        """Tests model status."""
        kmodel = ia.KMeansModel(name=model_name)
        self.assertEqual(kmodel.status, "ACTIVE")
        ia.drop_models(kmodel)
        self.assertEqual(kmodel.status, "DROPPED")

    def test_model_last_read_date(self):
        """Tests model last read date."""
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)
        old_read = kmodel.last_read_date
        kmodel.train(self.frame_train,
                     ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                     [1.0, 1.0, 1.0, 1.0, 1.0], 5)
        new_read = kmodel.last_read_date
        self.assertNotEqual(old_read, new_read)

    def test_kmeans_standard(self):
        """Tests standard usage of the kmeans cluster algorithm."""
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5)
        self._validate(result, kmodel)

    def test_column_weights(self):
        """Tests kmeans cluster algorithm with weighted values."""
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [0.1, 0.1, 0.1, 0.1, 0.1], 5)
        self._validate(result, kmodel, 834.0)

    def test_max_iterations(self):
        """Tests kmeans cluster algorithm with more iterations."""
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5, max_iterations=35)

        self._validate(result, kmodel)

    def test_epsilon_assign(self):
        """Tests kmeans cluster algorithm with an arbitrary epsilon. """
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        result = kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5, epsilon=.000000000001)
        self._validate(result, kmodel)

    def test_intialization_mode_random(self):
        """Tests kmeans cluster algorithm with random seeds."""
        # no asserts performed since this testcase is too random
        model_name = cu.get_a_name(self.prefix)
        kmodel = ia.KMeansModel(name=model_name)

        kmodel.train(
            self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
            [1.0, 1.0, 1.0, 1.0, 1.0], 5, initialization_mode="random")

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
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], 5, max_iterations=-3)

    def test_max_iterations_bad_type(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], 5, max_iterations=[])

    def test_k_negative(self):
        """Check error on negative number of clusters."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], -5)

    def test_k_bad_type(self):
        """Check error on float number of clusters."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], [])

    def test_epsilon_negative(self):
        """Check error on negative epsilon value."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], 5, epsilon=-0.05)

    def test_epsilon_bad_type(self):
        """Check error on bad epsilon type."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], 5, epsilon=[])

    def test_initialization_mode_bad_type(self):
        """Check error on bad initialization type."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(
                self.frame_train, ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                [0.01, 0.01, 0.01, 0.01, 0.01], 5, initialization_mode=3)

    def test_initialization_mode_bad_value(self):
        """Check error on bad initialization value."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01],
                         5, initialization_mode="badvalue")

    def test_invalid_columns_predict(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [1.0, 1.0, 1.0, 1.0, 1.0],
                         5, max_iterations=[])
            self.frame_test.rename_columns(
                {"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3",
                 "Vec4": "Dim4", "Vec5": 'Dim5'})
            kmodel.predict(self.frame_test)

    def test_too_few_columns(self):
        """Check error on a floating point number of iterations."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(self.frame_train,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [1.0, 1.0, 1.0, 1.0, 1.0],
                         5, max_iterations=[])
            kmodel.predict(self.frame_test, ["Vec1", "Vec2"])

    def test_null_frame(self):
        """Check error on null frame."""
        with(self.assertRaises(ia.rest.command.CommandServerError)):
            model_name = cu.get_a_name(self.prefix)
            kmodel = ia.KMeansModel(name=model_name)

            kmodel.train(None,
                         ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"],
                         [0.01, 0.01, 0.01, 0.01, 0.01], 5)

    def _validate(self, result, kmodel, val=83379.0):
        self.assertAlmostEqual(
            val, result['within_set_sum_of_squared_error'], delta=1000)
        for i in range(1, 6):
            self.assertEqual(result['cluster_size']['Cluster:'+str(i)], 10000)

        test_frame = kmodel.predict(self.frame_test)
        test_take = test_frame.download(test_frame.row_count)
        grouped = test_take.groupby(['predicted_cluster', 'term'])
        for i in grouped.size():
            self.assertEqual(10000, i)


if __name__ == '__main__':
    unittest.main()
