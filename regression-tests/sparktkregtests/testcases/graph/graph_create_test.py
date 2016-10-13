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


class GraphCreate(sparktk_test.SparkTKTestCase):

    edge_baseline = [[1, 2, 1], [2, 3, 1], [1, 4, 1], [5, 1, 1],
                     [3, 4, 1], [4, 5, 1], [4, 6, 1], [5, 6, 1]]

    vertex_baseline = [[1, 0.0, ".333"], [2, 0.0, "0"], [3, 0.0, "0"],
                       [4, .16667, ".3333"], [5, 1.0, ".6666"], [6, 1.0, "1"]]

    def setUp(self):
        edges_dataset = self.get_file("clustering_graph_edges.csv")
        vertex_dataset = self.get_file("clustering_graph_vertices.csv")

        edge_schema = [('src', int),
                       ('dst', int),
                       ('xit_cost', int)]
        vertex_schema = [('id', int),
                         ('prop1', float),
                         ('prop2', str)]

        self.edges = self.context.frame.import_csv(
            edges_dataset, schema=edge_schema)
        self.vertices = self.context.frame.import_csv(
            vertex_dataset, schema=vertex_schema)

    def test_graph_creation(self):
        """Build a simple non-bipartite graph"""
        graph = self.context.graph.create(self.vertices, self.edges)
        vertices = graph.create_vertices_frame()
        edges = graph.create_edges_frame()

        vertex_take = vertices.take(vertices.count())
        edge_take = edges.take(edges.count())

        self.assertEqual(6, graph.vertex_count())

        self.assertItemsEqual(self.edge_baseline, edge_take)
        self.assertItemsEqual(self.vertex_baseline, vertex_take)

    def test_graph_save_and_load(self):
        """Build a simple graph, save it, load it"""
        graph = self.context.graph.create(self.vertices, self.edges)
        file_path = self.get_file(self.get_name("graph"))
        graph.save(file_path)

        new_graph = self.context.load(file_path)

        vertices = new_graph.create_vertices_frame()
        edges = new_graph.create_edges_frame()
        vertex_take = vertices.take(vertices.count())
        edge_take = edges.take(edges.count())

        self.assertEqual(6, new_graph.vertex_count())

        self.assertItemsEqual(self.edge_baseline, edge_take)
        self.assertItemsEqual(self.vertex_baseline, vertex_take)

    def test_graph_graphframe(self):
        """Test the underlying graphframe is properly exposed"""
        graph = self.context.graph.create(self.vertices, self.edges)

        graph_frame = graph.graphframe.degrees
        degree_frame = self.context.frame.create(graph_frame)

        baseline = [[1, 3], [2, 2], [3, 2], [4, 4], [5, 3], [6, 2]]
        self.assertItemsEqual(
            baseline, degree_frame.take(degree_frame.count()))

    def test_graph_bad_src(self):
        """Test the underlying graphframe fails to build if missing src"""
        self.edges.rename_columns({"src": "bad"})

        with self.assertRaisesRegexp(ValueError, "\'src\' missing"):
            self.context.graph.create(self.vertices, self.edges)

    def test_graph_bad_dst(self):
        """Test the underlying graphframe fails to build if missing dst"""
        self.edges.rename_columns({"dst": "bad"})

        with self.assertRaisesRegexp(ValueError, "\'dst\' missing"):
            self.context.graph.create(self.vertices, self.edges)

    def test_graph_bad_ids(self):
        """Test the underlying graphframe fails to build if missing id"""
        self.vertices.rename_columns({"id": "bad"})

        with self.assertRaisesRegexp(ValueError, "\'id\' missing"):
            self.context.graph.create(self.vertices, self.edges)

if __name__ == "__main__":
    unittest.main()
