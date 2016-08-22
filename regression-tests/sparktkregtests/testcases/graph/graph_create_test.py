""" Construct a non-bipartite graph. Tests the graph creation"""

import unittest

from sparktkregtests.lib import sparktk_test


class GraphCreate(sparktk_test.SparkTKTestCase):

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
        self.vertices= self.context.frame.import_csv(
            vertex_dataset, schema=vertex_schema)

    def test_graph_creation(self):
        """Build a simple non-bipartite graph"""
        graph = self.context.graph.create(self.vertices, self.edges)

        self.assertEqual(6, graph.vertex_count())
        self.assertEqual(6, graph.create_vertices_frame().row_count)
        self.assertEqual(7, graph.create_edges_frame().row_count)

    def test_graph_save_and_load(self):
        """Build a simple graph, save it, load it"""
        graph = self.context.graph.create(self.vertices, self.edges)
        file_path = self.get_file(self.get_name("graph"))
        graph.save(file_path)

        new_graph = self.context.load(file_path)
        self.assertEqual(6, new_graph.vertex_count())
        self.assertEqual(6, new_graph.create_vertices_frame().row_count)
        self.assertEqual(7, new_graph.create_edges_frame().row_count)

    def test_graph_graphframe(self):
        """Test the underlying graphframe is properly exposed"""
        graph = self.context.graph.create(self.vertices, self.edges)

        graph_frame = graph.graphframe.degrees
        degree_frame = self.context.frame.create(graph_frame)

        baseline = [[1, 2], [2, 2], [3, 2], [4, 4], [5, 2], [6, 2]]
        self.assertItemsEqual(
            baseline, degree_frame.take(degree_frame.row_count).data)

if __name__ == "__main__":
    unittest.main()
