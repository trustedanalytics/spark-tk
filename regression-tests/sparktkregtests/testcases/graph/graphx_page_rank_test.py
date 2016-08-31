"""Tests PageRank exposed in ATK from graphx. Validated against networkx"""
import unittest
import operator

import networkx as nx

from sparktkregtests.lib import sparktk_test


class PageRank(sparktk_test.SparkTKTestCase):

    MAX_ITERATIONS = 20
    CONVERGENCE_TOLERANCE = 0.001
    TOP_N_RANKS = 20

    def setUp(self):
        """Creates both frame and graph is used by multiple tests."""
        super(PageRank, self).setUp()
        graph_data = self.get_file("clique_10.csv")
        schema = [('src', str),
                  ('dst', str)]

        # set up the vertex frame, which is the union of the src and
        # the dst columns of the edges
        self.frame = self.context.frame.import_csv(graph_data, schema=schema)
        self.vertices = self.frame.copy()
        self.vertices2 = self.frame.copy()
        self.vertices.rename_columns({"src": "id"})
        self.vertices.drop_columns(["dst"])
        self.vertices2.rename_columns({"dst": "id"})
        self.vertices2.drop_columns(["src"])
        self.vertices.append(self.vertices2)
        self.vertices.drop_duplicates()

        self.graph = self.context.graph.create(self.vertices, self.frame)

    def test_string_in_max_iterations(self):
        """Negative test to verify integer type for max_iterations."""
        with self.assertRaises(Exception):
            self.graph.page_rank(max_iterations="20")

    def test_string_convergence_tol(self):
        """Negative test to verify convergence_tolerance type is checked."""
        with self.assertRaises(Exception):
            self.graph.page_rank(
                max_iterations=0, convergence_tolerance="0.001")

    def test_page_rank_networkx_comparison_parquet(self):
        """ Tests the graphx pagerank exposed in ATK on parquet graph"""
        result = self.graph.page_rank(
            convergence_tolerance=self.CONVERGENCE_TOLERANCE)

        pandas_vertices = result.download(result.count())
        edges_frame = self.graph.create_edges_frame()

        edge_list = map(tuple, edges_frame.take(edges_frame.count()).data)

        G = nx.Graph()
        G.add_edges_from(edge_list)
        nx_pagerank = nx.pagerank(G, max_iter=self.MAX_ITERATIONS, tol=self.CONVERGENCE_TOLERANCE)

if __name__ == '__main__':
    unittest.main()
