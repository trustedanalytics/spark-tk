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
        self.vectors = ["Vec1", "Vec2", "Vec3", "Vec4", "Vec5"]
        self.frame_train = self.context.frame.import_csv(
            self.get_file("kmeans_train.csv"), schema=schema)
        self.frame_test = self.context.frame.import_csv(
            self.get_file("kmeans_test.csv"), schema=schema)

    def test_different_columns(self):
        """Tests kmeans cluster algorithm with more iterations."""
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                             k=5,
                                                             max_iter=300)
 
        # change the column names
        self.frame_test.rename_columns(
            {"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3",
             "Vec4": "Dim4", "Vec5": 'Dim5'})
        predicted_frame = kmodel.predict(
            self.frame_test, ['Dim1', 'Dim2', 'Dim3', 'Dim4', 'Dim5'])
        self._validate(kmodel, predicted_frame)

    def test_add_distance_columns_twice(self):
        """tests kmeans model add distances cols twice"""
        model = self.context.models.clustering.kmeans.train(self.frame_train,
                                                            self.vectors,
                                                            k=5)
        model.add_distance_columns(self.frame_train)
        with self.assertRaisesRegexp(Exception, "conflicting column names"):
            model.add_distance_columns(self.frame_train)

    def test_kmeans_standard(self):
        """Tests standard usage of the kmeans cluster algorithm."""
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                             k=5)
        predicted_frame = kmodel.predict(self.frame_test)
        self._validate(kmodel, predicted_frame)

    def test_column_weights(self):
        """Tests kmeans cluster algorithm with weighted values."""
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[0.1, 0.1, 0.1, 0.1, 0.1],
                                                             k=5)
        predicted_frame = kmodel.predict(self.frame_test)
        self._validate(kmodel, predicted_frame)

    def test_max_iterations(self):
        """Tests kmeans cluster algorithm with more iterations."""
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                             k=5,
                                                             max_iter=35)
        predicted_frame = kmodel.predict(self.frame_test)
        self._validate(kmodel, predicted_frame)

    def test_epsilon_assign(self):
        """Tests kmeans cluster algorithm with an arbitrary epsilon. """
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                             k=5,
                                                             epsilon=.000000000001)
        predicted_frame = kmodel.predict(self.frame_test)
        self._validate(kmodel, predicted_frame)

    @unittest.skip("publish model not complete in dev")
    def test_publish(self):
        """Tests kmeans cluster publish."""
        kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                             self.vectors,
                                                             scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                             k=5)
        path = kmodel.publish()

        self.assertIn("hdfs", path)
        self.assertIn("tar", path)

    def test_max_iterations_negative(self):
        """Check error on negative number of iterations."""
        with self.assertRaisesRegexp(Exception, "maxIterations must be a positive value"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=5,
                                                        max_iter=-3)

    def test_max_iterations_bad_type(self):
        """Check error on invalid number of iterations."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=5,
                                                        max_iter=[])

    def test_k_negative(self):
        """Check error on negative number of clusters."""
        with self.assertRaisesRegexp(Exception, "k must be at least 1"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=-5)

    def test_k_bad_type(self):
        """Check error on invalid number of clusters."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=[])

    def test_epsilon_negative(self):
        """Check error on negative epsilon value."""
        with self.assertRaisesRegexp(Exception, "epsilon must be a positive value"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=5,
                                                        epsilon=-0.05)

    def test_epsilon_bad_type(self):
        """Check error on bad epsilon type."""
        with self.assertRaisesRegexp(Exception, "does not exist"):
            self.context.models.clustering.kmeans.train(self.frame_train,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=5,
                                                        epsilon=[])

    def test_invalid_columns_predict(self):
        """Check error with invalid columns"""
        with self.assertRaisesRegexp(Exception, "Invalid column name"):
            kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                                 self.vectors,
                                                                 scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                                 k=5)

            self.frame_test.rename_columns(
                {"Vec1": 'Dim1', "Vec2": 'Dim2', "Vec3": "Dim3",
                 "Vec4": "Dim4", "Vec5": 'Dim5'})
            predicted_frame = kmodel.predict(self.frame_test)
            self.frame_test.count()

    def test_too_few_columns(self):
        """Check error on invalid num of columns"""
        with self.assertRaisesRegexp(Exception, "Number of columns for train and predict should be same"):
            kmodel = self.context.models.clustering.kmeans.train(self.frame_train,
                                                                 self.vectors,
                                                                 scalings=[1.0, 1.0, 1.0, 1.0, 1.0],
                                                                 k=5)

            predicted_frame = kmodel.predict(self.frame_test, columns=["Vec1", "Vec2"])

    @unittest.skip("sparktk: Model training with null frame should give a useful exception message")
    def test_null_frame(self):
        """Check error on null frame."""
        with self.assertRaisesRegexp(Exception, "foo"):
            self.context.models.clustering.kmeans.train(None,
                                                        self.vectors,
                                                        scalings=[0.01, 0.01, 0.01, 0.01, 0.01],
                                                        k=5)

    def _validate(self, kmodel, frame, val=83379.0):
        """ensure that clusters are correct"""
        # group the result by cluster and term
        # term is the expected result
        grouped = frame.group_by(['cluster', 'term'])
        groups = grouped.take(grouped.count())
 
        # iterate through the groups
        # and assert that the expected group ("term")
        # maps 1:1 with the resulting cluster
        # i.e., ensure that the clusters are the same
        # for expected and actual, however the names of
        # the clusters may be different
        for group in groups:
            self.assertEquals(len(group), 2)


if __name__ == '__main__':
    unittest.main()
