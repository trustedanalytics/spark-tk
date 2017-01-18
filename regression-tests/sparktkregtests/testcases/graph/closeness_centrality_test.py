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

"""Tests closeness centrality algorithm for graphs"""
import unittest
from sparktkregtests.lib import sparktk_test

class ClosenessCentrality(sparktk_test.SparkTKTestCase):

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
        result_frame = self.graph.closeness_centrality(normalize=False)
        result = result_frame.to_pandas()
        
        #validate centrality values
        expected_values = {0 : 0.5,
            1: 0.0,
            2: 0.667,
            3: 0.75,
            4: 1.0,
            5: 0.0,
            6: 0.0}

        self._validate_result(result, expected_values)

    def test_weights_single_shortest_path(self):
        """Tests weighted closeness when only one shortest path present""" 
        edges = self.context.frame.create(
            [(0,1,3), (0, 2, 2),
            (0, 3, 6), (0, 4, 4),
            (1, 3, 5), (1, 5, 5),
            (2, 4, 1), (3, 4, 2),
            (3, 5, 1), (4, 5, 4)],
            ["src", "dst", "weights"])
        vertices = self.context.frame.create([[0], [1], [2], [3], [4], [5]], ["id"])
        graph = self.context.graph.create(vertices, edges)

        #validate centrality values
        result_frame = graph.closeness_centrality("weights", False)
        result = result_frame.to_pandas()
        expected_values = {0 : 0.238,
            1: 0.176,
            2: 0.333,
            3: 0.667,
            4: 0.25,
            5: 0.0}
        self._validate_result(result, expected_values)


    def test_weights_multiple_shortest_paths(self):
        """Test cetality when multiple shortest paths exist"""
        result_frame = self.graph.closeness_centrality("weights", False)

        #validate centrality values
        expected_values = {0 : 0.261,
            1: 0.0,
            2: 0.235,
            3: 0.333,
            4: 0.667,
            5: 0.0,
            6: 0.0}
        result = result_frame.to_pandas()
        self._validate_result(result, expected_values)

    def test_disconnected_edges(self):
        """Test closeness on graph with disconnected edges"""
        edges = self.context.frame.create(
            [['a', 'b'], ['a', 'c'],
            ['c', 'd'], ['c', 'e'],
            ['f', 'g'], ['g', 'h']],            
            ['src', 'dst'])
        vertices = self.context.frame.create(
            [['a'], ['b'], ['c'], ['d'], ['e'], ['f'], ['g'], ['h']],
            ['id'])
        graph = self.context.graph.create(vertices, edges)
        result_frame = graph.closeness_centrality(normalize=False)
        
        #validate centrality values
        expected_values = {'a': 0.667,
            'b': 0.0, 'c': 1.0, 'd': 0.0,
            'e': 0.0, 'f': 0.667, 'g': 1.0, 'h':0.0}
    
        result = result_frame.to_pandas()
        self._validate_result(result, expected_values)
 
    def test_normalize(self):
        """Test normalized centrality"""
        result_frame = self.graph.closeness_centrality(normalize=True)
        result = result_frame.to_pandas()
        
        #validate centrality values
        expected_values = {0 : 0.5,
            1: 0.0,
            2: 0.444,
            3: 0.375,
            4: 0.333,
            5: 0.0,
            6: 0.0}
        self._validate_result(result, expected_values)


    def test_negative_edges(self):
        """Test closeness on graph with disconnected edges"""
        edges = self.context.frame.create(
            [['a', 'b', 10], ['a', 'c', 12],
            ['c', 'd', -1], ['c', 'e', 5]],
            ['src', 'dst', 'weight'])
        vertices = self.context.frame.create(
            [['a'], ['b'], ['c'], ['d'], ['e']],
            ['id'])
        graph = self.context.graph.create(vertices, edges)

        with self.assertRaisesRegexp(
                Exception, "edge weight cannot be negative"):
            graph.closeness_centrality(
                edge_prop_name='weight',
                normalize=False)

    def test_bad_weights_column_name(self):
        """Should throw exception when bad weights column name given"""
        with self.assertRaisesRegexp(
                Exception, "Field \"BAD\" does not exist"):
            self.graph.closeness_centrality("BAD")

    def _validate_result(self, result, expected_values):
        for i, row in result.iterrows():
            id = row['id']
            self.assertAlmostEqual(
                row["closeness_centrality"],
                expected_values[id],
                delta = 0.1)
if __name__ == "__main__":
    unittest.main()

