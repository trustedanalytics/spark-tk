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

"""Tests betweenness centrality algorithm for graphs"""
import unittest
from sparktkregtests.lib import sparktk_test


class BetweennessCentrality(sparktk_test.SparkTKTestCase):

    def setUp(self):
        edges = self.context.frame.create(
            [(0, 1, 1),
             (0, 2, 1),
             (2, 3, 2),
             (2, 4, 4),
             (3, 4, 2),
             (3, 5, 4),
             (4, 5, 2),
             (4, 6, 1)],
            ["src", "dst", "weights"])

        vertices = self.context.frame.create(
            [[0], [1], [2], [3], [4], [5], [6]], ["id"])

        self.graph = self.context.graph.create(vertices, edges)

    def test_default(self):
        """Test default settings"""
        result_frame = self.graph.betweenness_centrality()
        result = result_frame.to_pandas()

        # validate centrality values
        expected_value = {
            0: 0.333,
            1: 0.0,
            2: 0.533,
            3: 0.1,
            4: 0.433,
            5: 0.0,
            6: 0.0}

        for i, row in result.iterrows():
            id = row['id']
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_value[id],
                delta=0.001)

    def test_weights_single_shortest_path(self):
        """Tests weighted betweenness when only one shortest path present"""
        edges = self.context.frame.create(
            [(0, 1, 3), (0, 2, 2),
             (0, 3, 6), (0, 4, 4),
             (1, 3, 5), (1, 5, 5),
             (2, 4, 1), (3, 4, 2),
             (3, 5, 1), (4, 5, 4)],
            ["src", "dst", "weights"])
        vertices = self.context.frame.create(
            [[0], [1], [2], [3], [4], [5]], ["id"])
        graph = self.context.graph.create(vertices, edges)

        # validate against values from networkx betweenness centrality
        result_frame = graph.betweenness_centrality("weights", False)
        result = result_frame.to_pandas()
        expected_values = {
            0: 2.0,
            1: 0.0,
            2: 4.0,
            3: 3.0,
            4: 4.0,
            5: 0.0}

        for i, row in result.iterrows():
            id = row['id']
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[id],
                delta=0.1)

    @unittest.skip("Bug:DPNG-14802")
    def test_weights(self):
        """Test betweenness with weighted cost"""
        result_frame = self.graph.betweenness_centrality("weights", False)

        # validate betweenness centrality values
        expected_values = [0.0, 5.0, 0.0, 0.0, 8.0, 5.0, 7.5]
        result = result_frame.to_pandas()
        for i, row in result.iterrows():
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[i],
                delta=0.1)

    def test_disconnected_edges(self):
        """Test betweenness on graph with disconnected edges"""
        edges = self.context.frame.create(
            [['a', 'b'], ['a', 'c'],
             ['c', 'd'], ['c', 'e'],
             ['f', 'g'], ['g', 'h']],
            ['src', 'dst'])

        vertices = self.context.frame.create(
            [['a'], ['b'], ['c'], ['d'], ['e'], ['f'], ['g'], ['h']], ['id'])

        graph = self.context.graph.create(vertices, edges)

        result_frame = graph.betweenness_centrality(normalize=False)

        # validate betweenness centrality values
        expected_values = {
            'a': 3.0,
            'b': 0.0, 'c': 5.0, 'd': 0.0,
            'e': 0.0, 'f': 0.0, 'g': 1.0, 'h': 0.0}

        result = result_frame.to_pandas()
        for i, row in result.iterrows():
            id = row['id']
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[id],
                delta=0.1)

    def test_normalize(self):
        """Test unnomallized betweenness crentrality"""
        result_frame = self.graph.betweenness_centrality(normalize=False)
        result = result_frame.to_pandas()

        # validate centrality values
        expected_values = {
            0: 5.0,
            1: 0.0,
            2: 8.0,
            3: 1.5,
            4: 6.5,
            5: 0.0,
            6: 0.0}

        for i, row in result.iterrows():
            id = row['id']
            self.assertAlmostEqual(
                row["betweenness_centrality"],
                expected_values[id],
                delta=0.1)

    def test_bad_weights_column_name(self):
        """Should throw exception when bad weights column name given"""
        with self.assertRaisesRegexp(
                Exception, "Field \"BAD\" does not exist"):
            self.graph.betweenness_centrality("BAD")


if __name__ == "__main__":
    unittest.main()
