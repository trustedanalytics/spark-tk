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

"""Test power iteration Clustering against known values"""
import math
import unittest
from sparktkregtests.lib import sparktk_test


class PowerIterationTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Import the files to be tested."""
        super(PowerIterationTest, self).setUp()
        data = self.get_file("pic_circles_data.csv")
        self.schema = [('Source', int),
                       ('Destination', int),
                       ('Similarity', float)]

        self.frame = self.context.frame.import_csv(data, schema=self.schema)

    def test_doc_example(self):
        """ Example from the API documentation """
        data = [[1,2,1.0],
                [1,3,0.3],
                [2,3,0.3],
                [3,0,0.03],
                [0,5,0.01],
                [5,4,0.3],
                [5,6,1.0],
                [4,6,0.3]]

        frame = self.context.frame.create(data, schema=self.schema)
        result = frame.power_iteration_clustering(
            "Source", "Destination", "Similarity", k=3, max_iterations=10)

        #check cluster sizes
        actual_cluster_sizes = sorted(result.cluster_sizes.values())
        expected_cluster_sizes = [1, 3, 3]
        self.assertItemsEqual(actual_cluster_sizes, expected_cluster_sizes)

        #check values assigned to each cluster
        actual_assignment = result.frame.to_pandas(
            frame.count()).values.tolist()
        expected_assignment = [[4, 2], [0, 3], [1, 1],
            [5, 2], [6, 2], [2, 1], [3, 1]]
        self.assertItemsEqual(actual_assignment, expected_assignment)

    #@unittest.skip("bug:max_iter 100 causes stackoverflow")
    def test_circles_default(self):
        """ Test pic on similarity matrix for two concentric cicles """
        result = self.frame.power_iteration_clustering(
            "Source", "Destination", "Similarity", k=2)

        #check cluster sizes
        actual_cluster_sizes = sorted(result.cluster_sizes.values())
        expected_cluster_sizes = [5, 15]
        self.assertItemsEqual(actual_cluster_sizes, expected_cluster_sizes)

        #check values assigned to each cluster
        actual_assignment = result.frame.to_pandas(
            result.frame.count()).values.tolist()
        expected_assignment = \
            [[4,1], [16,1], [14,1], [0,1], [6,1], [8,1],
            [12,1], [18,1], [10,1], [2,1], [13,2], [19,2],
            [15,2], [11,2], [1,1], [17,2], [3,1], [7,1], [9,1], [5,1]]
        self.assertItemsEqual(actual_assignment, expected_assignment)

    def test_circles_max_iterations(self):
        """ Test pic with max_iteration = 40 """
        result = self.frame.power_iteration_clustering(
            "Source", "Destination", "Similarity", k=2, max_iterations=40)

        #check cluster sizes
        actual_cluster_sizes = sorted(result.cluster_sizes.values())
        expected_cluster_sizes = [10, 10]
        self.assertItemsEqual(actual_cluster_sizes, expected_cluster_sizes)

        #check values assigned to each cluster
        actual_assignment = result.frame.to_pandas(
            result.frame.count()).values.tolist()
        expected_assignment = \
            [[4,1], [16,2], [14,2], [0,1], [6,1], [8,1],
            [12,2], [18,2], [10,2], [2,1], [13,2], [19,2],
            [15,2], [11,2], [1,1], [17,2], [3,1], [7,1], [9,1], [5,1]]  
        self.assertItemsEqual(actual_assignment, expected_assignment)


    def test_neg_similarity(self):
        """ Test pic with negative similarity values """
        bad_frame = self.context.frame.create(
            [[1, 0, -1.0], [1, 2, 0.1]], schema=self.schema)

        with self.assertRaisesRegexp(
                Exception, "Similarity must be nonnegative but found .*"):
            result = bad_frame.power_iteration_clustering(
                "Source", "Destination", "Similarity", k=2, max_iterations=10)

    def test_bad_column_name(self):
        """ Test behavior for bad source column name """
        with self.assertRaisesRegexp(
                Exception, "Invalid column name ERR .*"):
            result = self.frame.power_iteration_clustering(
                "ERR", "Destination", "Similarity")

    def test_bad_k_value(self):
        """ Tests behavior for bad number of k """
        with self.assertRaisesRegexp(
                Exception,
                "Number of clusters must be must be greater than 1"):
            result = self.frame.power_iteration_clustering(
                "Source", "Destination", "Similarity", k=0)

    def test_bad_max_iterations(self):
        """ Tests behavior for negative max_iterations """
        with self.assertRaisesRegexp(
                Exception,
                "Maximum number of iterations must be greater than 0"):
            result = self.frame.power_iteration_clustering(
                "Source", "Destination", "Similarity", k=2,
                max_iterations=-1)
if __name__ == "__main__":
    unittest.main()
