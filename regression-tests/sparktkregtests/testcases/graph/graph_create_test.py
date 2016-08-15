""" Construct a non-bipartite graph. Tests the graph creation"""

import unittest

from sparktkregtests.lib import sparktk_test


class GraphCreate(sparktk_test.SparkTKTestCase):

    def test_graph_creation(self):
        """Build a simple non-bipartite graph"""
        edges_dataset = self.get_file("clustering_graph_edges.csv")
        nodes_dataset = self.get_file("clustering_graph_nodes.csv")

        edge_schema = [('src', int),
                       ('dst', int),
                       ('xit_cost', int)]
        node_schema = [('id', int),
                       ('prop1', float),
                       ('prop2', str)]

        edges = self.context.frame.import_csv(
            edges_dataset, schema=edge_schema)
        nodes = self.context.frame.import_csv(
            nodes_dataset, schema=node_schema)

        graph = self.context.graph.create(nodes, edges)

        self.assertEqual(6, graph.vertex_count())
        self.assertEqual(6, graph.create_vertices_frame().row_count)
        self.assertEqual(7, graph.create_edges_frame().row_count)


if __name__ == "__main__":
    unittest.main()
