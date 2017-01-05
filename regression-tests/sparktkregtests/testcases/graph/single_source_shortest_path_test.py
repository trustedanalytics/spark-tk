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

""" Construct a non-bipartite graph. Tests the graph creation"""

import unittest
from math import isinf
from sparktkregtests.lib import sparktk_test


class SSSP(sparktk_test.SparkTKTestCase):


    def setUp(self):
        edges = self.context.frame.create(
            [("ar", "si", 140),
            ("ar", "ze", 75),
            ("ze", "or", 71),
            ("or", "si", 151),
            ("si", "fa", 99),
            ("fa", "bu", 211),
            ("bu", "pi", 101),
            ("bu", "gi", 90),
            ("pi", "rv", 97),
            ("rv", "si", 80),
            ("pi", "cr", 138),
            ("cr", "do", 120),
            ("do", "me", 75),
            ("me", "lu", 70),
            ("lu", "ti", 111),
            ("ti", "ar", 118)],
            ["src", "dst", "distance"])

        vertex = self.context.frame.create(
            [("ar", "arad"),
            ("ze", "zerind"),
            ("or", "oradea"),
            ("si", "sibiu"),
            ("fa", "fagaras"),
            ("bu", "bucharest"),
            ("pi", "pitesti"),
            ("gi", "giurgiu"),
            ("rv", "rimnicu vilcea"),
            ("cr", "craiova"),
            ("do", "dobreta"),
            ("me", "mehadia"),
            ("lu", "lugoj"),
            ("ti", "timisoara"),
            ("ne", "neamt"),
            ("ia", "iasi")],
            ["id", "city"])

        self.graph = self.context.graph.create(vertex, edges)

    def test_default_weight(self):
        """Tests sssp with default weight"""
        result_frame = self.graph.single_source_shortest_path("bu")
        
        #validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_path =\
            [['bu', ' pi', ' rv'],
            ['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti', ' ar',
            ' ze', ' or'],
            ['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti', ' ar'],
            ['bu', ' pi', ' cr'],
            ['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti'],
            ['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti', ' ar', ' ze'],
            ['bu', ' pi', ' cr', ' do', ' me'],
            ['bu'], [''], ['bu', ' pi', ' rv', ' si'],
            ['bu', ' pi', ' cr', ' do', ' me', ' lu'],
            ['bu', ' gi'], ['bu', ' pi', ' rv', ' si', ' fa'], [''],
            ['bu', ' pi'], ['bu', ' pi', ' cr', ' do']]
        expected_cost = [2.0, 9.0, 7.0, 2.0, 6.0, 8.0, 4.0, 
            0.0, float("inf"), 3.0, 5.0, 1.0, 4.0, float("inf"), 1.0, 3.0]
        self._validate_result(result, expected_path, expected_cost)

    def test_edge_weight(self):
        """Tests sssp with distance field as weight"""
        result_frame = self.graph.single_source_shortest_path("ar", "distance")

        #validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_path =\
            [['ar', ' si', ' fa', ' bu', ' pi', ' rv'],
            ['ar', ' ze', ' or'], ['ar'],
            ['ar', ' si', ' fa', ' bu', ' pi', ' cr'],
            ['ar', ' si', ' fa', ' bu', ' pi', ' cr',' do',
            ' me', ' lu', ' ti'],
            ['ar', ' ze'],
            ['ar', ' si', ' fa', ' bu', ' pi', ' cr', ' do', ' me'], 
            ['ar', ' si', ' fa', ' bu'], [''], ['ar', ' si'],
            ['ar', ' si', ' fa', ' bu', ' pi', ' cr', ' do', ' me', ' lu'],
            ['ar', ' si', ' fa', ' bu', ' gi'], ['ar', ' si', ' fa'], [''],
            ['ar', ' si', ' fa', ' bu', ' pi'], ['ar', ' si', ' fa', ' bu',
            ' pi', ' cr', ' do']]

        expected_cost =\
            [648.0, 146.0, 0.0, 689.0, 1065.0, 75.0,
            884.0, 450.0, float("inf"), 140.0, 954.0,
            540.0, 239.0, float("inf"), 551.0, 809.0]

        self._validate_result(result, expected_path, expected_cost)

    def test_max_path_length(self):
        """Tests sssp with max_path_length of 500"""
        result_frame = self.graph.single_source_shortest_path("ar", "distance", 500)

        #validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_path =\
            [[''], ['ar', ' ze', ' or'], ['ar'], [''], [''],
            ['ar', ' ze'], [''], ['ar', ' si', ' fa', ' bu'],
            [''], ['ar', ' si'], [''], [''], ['ar', ' si', ' fa'],
            [''], [''], ['']]

        expected_cost =\
            [float("inf"), 146.0, 0.0, float("inf"), float("inf"), 75.0,
            float("inf"), 450.0, float("inf"), 140.0, float("inf"),
            float("inf"), 239.0, float("inf"), float("inf"), float("inf")]

        self._validate_result(result, expected_path, expected_cost)


    @unittest.skip("Bug DPNG-14799")
    def test_diconnected_src(self):
        """Tests sssp with a disconnected node as source"""
        result_frame = self.graph.single_source_shortest_path("ne")

        #validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_path =\
            [[''], [''], [''], [''], [''],
            [''], [''], [''],
            ['ne'], [''], [''], [''], [''],
            [''], [''], ['']]


        expected_cost =\
            [float("inf"), float("inf"), float("inf"), float("inf"), float("inf"),
            float("inf"), float("inf"), float("inf"), 0.0, float("inf"),
            float("inf"), float("inf"), float("inf"), float("inf"), float("inf")]

        self._validate_result(result, expected_path, expected_cost)

    @unittest.skip("Bug DPNG-14800")
    def test_graph_with_negative_weights(self):
        """Tests SSSP on graph with negative weights"""
        edges = self.context.frame.create(
            [["1", "2", 2], ["2", "3", 2], ["2", "4", -1],
            ["3", "4", -5], ["4", "1", -3]],
            ['src', 'dst', 'weight'])
        vertices = self.context.frame.create(
            [["1"], ["2"], ["3"], ["4"]], ['id'])
        graph = self.context.graph.create(vertices, edges)

        result_frame = graph.single_source_shortest_path("1", "weight")
        result = result_frame.to_pandas()

        expected_path = [["1"], ["1", "2"], ["1", "2","3"], ["1", "2", "3", "4"]]
        expected_cost = [0, 2, 4, -1]
        self._validate_result(result, expected_path, expected_cost)

    @unittest.skip("Bug DPNG 14817")
    def test_int_src_vertex_id(self):
        """Tests SSSP with integer src_vertex_ids"""
        edges = self.context.frame.create(
            [[1, 2], [2, 3], [2, 4],
            [3, 4], [4, 1]],
            ['src', 'dst'])
        vertices = self.context.frame.create(
            [[1], [2], [3], [4]], ['id'])
        graph = self.context.graph.create(vertices, edges)

        result_frame = graph.single_source_shortest_path(1)
        result = result_frame.to_pandas()

        expected_path = [[1], [1, 2], [1, 2, 3], [1, 2, 4]]
        expected_cost = [0, 1, 2, 2]
        self._validate_result(result, expected_path, expected_cost)
        
    def test_bad_source_id(self):
        """Test sssp throws exception for bad source id"""
        with self.assertRaisesRegexp(
                Exception, "vertex ID which does not exist"):
            self.graph.single_source_shortest_path("BAD")

    def test_bad_edge_prop_name(self):
        """Test sssp throws exception for bad edge_prop_name"""
        with self.assertRaisesRegexp(
                Exception, "Field \"BAD\" does not exist"):
            self.graph.single_source_shortest_path("ar", "BAD")

    def _validate_result(self, result, expected_path, expected_cost):
        for i, row in result.iterrows():
            path = str(row["path"]).strip("[]").split(",")
            self.assertItemsEqual(path, expected_path[i])
            self.assertEqual(row["cost"], expected_cost[i])
            expected_cost.append(row['cost'])
        
if __name__ == "__main__":
    unittest.main()
