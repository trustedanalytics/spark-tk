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

        # validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_result = {
            'rv': [['bu', ' pi', ' rv'], 2.0],
            'or': [['bu', ' pi', ' cr', ' do', ' me',
                    ' lu', ' ti', ' ar', ' ze', ' or'], 9.0],
            'ar': [['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti', ' ar'],
                   7.0],
            'cr': [['bu', ' pi', ' cr'], 2.0],
            'ti': [['bu', ' pi', ' cr', ' do', ' me', ' lu', ' ti'], 6.0],
            'ze': [['bu', ' pi', ' cr', ' do', ' me',
                    ' lu', ' ti', ' ar', ' ze'], 8.0],
            'me': [['bu', ' pi', ' cr', ' do', ' me'], 4.0],
            'bu': [['bu'], 0.0],
            'ia': [[''], float('inf')],
            'si': [['bu', ' pi', ' rv', ' si'], 3.0],
            'lu': [['bu', ' pi', ' cr', ' do', ' me', ' lu'], 5.0],
            'gi': [['bu', ' gi'], 1.0],
            'fa': [['bu', ' pi', ' rv', ' si', ' fa'], 4.0],
            'ne': [[''], float('inf')],
            'pi': [['bu', ' pi'], 1.0],
            'do': [['bu', ' pi', ' cr', ' do'], 3.0]}

        self._validate_result(result, expected_result)

    def test_edge_weight(self):
        """Tests sssp with distance field as weight"""
        result_frame = self.graph.single_source_shortest_path("ar", "distance")

        # validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_result = {
            'rv': [['ar', ' si', ' fa', ' bu', ' pi', ' rv'], 648.0],
            'or': [['ar', ' ze', ' or'], 146.0],
            'ar': [['ar'], 0.0],
            'cr': [['ar', ' si', ' fa', ' bu', ' pi', ' cr'], 689.0],
            'ti': [['ar', ' si', ' fa', ' bu', ' pi', ' cr', ' do',
                    ' me', ' lu', ' ti'], 1065.0],
            'ze': [['ar', ' ze'], 75.0],
            'me': [['ar', ' si', ' fa', ' bu', ' pi', ' cr', ' do', ' me'],
                   884.0],
            'bu': [['ar', ' si', ' fa', ' bu'], 450.0],
            'ia': [[''], float('inf')],
            'si': [['ar', ' si'], 140.0],
            'lu': [['ar', ' si', ' fa', ' bu', ' pi',
                    ' cr', ' do', ' me', ' lu'], 954.0],
            'gi': [['ar', ' si', ' fa', ' bu', ' gi'], 540.0],
            'fa': [['ar', ' si', ' fa'], 239.0],
            'ne': [[''], float('inf')],
            'pi': [['ar', ' si', ' fa', ' bu', ' pi'], 551.0],
            'do': [['ar', ' si', ' fa', ' bu',
                    ' pi', ' cr', ' do'], 809.0]}

        self._validate_result(result, expected_result)

    def test_max_path_length(self):
        """Tests sssp with max_path_length of 500"""
        result_frame = self.graph.single_source_shortest_path(
            "ar", "distance", 500.0)

        # validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_result = {
            'rv': [[''], float('inf')],
            'or': [['ar', ' ze', ' or'], 146.0],
            'ar': [['ar'], 0.0],
            'cr': [[''], float('inf')],
            'ti': [[''], float('inf')],
            'ze': [['ar', ' ze'], 75.0],
            'me': [[''], float('inf')],
            'bu': [['ar', ' si', ' fa', ' bu'], 450.0],
            'ia': [[''], float('inf')],
            'si': [['ar', ' si'], 140.0],
            'lu': [[''], float('inf')],
            'gi': [[''], float('inf')],
            'fa': [['ar', ' si', ' fa'], 239.0],
            'ne': [[''], float('inf')],
            'pi': [[''], float('inf')],
            'do': [[''], float('inf')]}

        self._validate_result(result, expected_result)

    def test_diconnected_src(self):
        """Tests sssp with a disconnected node as source"""
        result_frame = self.graph.single_source_shortest_path("ne")

        # validate number of rows in the result
        self.assertEqual(result_frame.count(), 16)

        result = result_frame.to_pandas()
        expected_result = {
            'rv': [[''], float('inf')],
            'or': [[''], float('inf')],
            'ar': [[''], float('inf')],
            'cr': [[''], float('inf')],
            'ti': [[''], float('inf')],
            'ze': [[''], float('inf')],
            'me': [[''], float('inf')],
            'bu': [[''], float('inf')],
            'ia': [[''], float('inf')],
            'si': [[''], float('inf')],
            'lu': [[''], float('inf')],
            'gi': [[''], float('inf')],
            'fa': [[''], float('inf')],
            'ne': [['ne'], 0.0],
            'pi': [[''], float('inf')],
            'do': [[''], float('inf')]}

        self._validate_result(result, expected_result)

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

        expected_result = {
            "1": [['1'], 0.0],
            "2": [['1', ' 2'], 2.0],
            "3": [['1', ' 2', ' 3'], 4.0],
            "4": [['1', ' 2', ' 3', ' 4'], -1]}

        self._validate_result(result, expected_result)

    def test_int_src_vertex_id(self):
        """Tests SSSP with integer src_vertex_ids"""
        edges = self.context.frame.create(
            [[1, 2], [2, 3], [2, 4], [3, 4], [4, 1]],
            ['src', 'dst'])

        vertices = self.context.frame.create(
            [[1], [2], [3], [4]], ['id'])

        graph = self.context.graph.create(vertices, edges)

        result_frame = graph.single_source_shortest_path(1)
        result = result_frame.to_pandas()

        expected_result = {
            1: [['1'], 0.0],
            2: [['1', ' 2'], 1.0],
            3: [['1', ' 2', ' 3'], 2.0],
            4: [['1', ' 2', ' 4'], 2.0]}

        self._validate_result(result, expected_result)

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

    def _validate_result(self, result, expected_result):

        for i, row in result.iterrows():
            id = row['id']
            path = str(row["path"]).strip("[]").split(",")
            self.assertItemsEqual(path, expected_result[id][0])
            self.assertEqual(row["cost"], expected_result[id][1])


if __name__ == "__main__":
    unittest.main()
