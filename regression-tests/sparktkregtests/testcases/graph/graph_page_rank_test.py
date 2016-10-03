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

"""Tests PageRank exposed from graphx. Validated against networkx"""
import unittest

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
        with self.assertRaisesRegexp(Exception, "cannot be cast"):
            self.graph.page_rank(max_iterations="20")

    def test_string_convergence_tol(self):
        """Negative test to verify convergence_tolerance type is checked."""
        with self.assertRaisesRegexp(Exception, "cannot be cast"):
            self.graph.page_rank(convergence_tolerance="0.001")

    def test_max_and_tol(self):
        """Negative test to verify convergence_tolerance type is checked."""
        with self.assertRaisesRegexp(
            Exception,
            "Only set one of max iterations or convergence tolerance"):
            self.graph.page_rank(
                max_iterations=20, convergence_tolerance=0.001)

    def test_no_max_or_tol(self):
        """Negative test to verify convergence_tolerance type is checked."""
        with self.assertRaisesRegexp(
            Exception,
            "Must set one of max iterations or convergence tolerance"):
            self.graph.page_rank()

    def test_page_rank_networkx_comparison(self):
        """ Tests the graphx pagerank exposed in graph"""
        result = self.graph.page_rank(
            convergence_tolerance=self.CONVERGENCE_TOLERANCE)

        pandas_vertices = result.to_pandas(result.count())
        edges_frame = self.graph.create_edges_frame()

        edge_list = map(tuple, edges_frame.take(edges_frame.count()))

        G = nx.Graph()
        G.add_edges_from(edge_list)
        nx_pagerank = nx.pagerank(G, max_iter=self.MAX_ITERATIONS, tol=self.CONVERGENCE_TOLERANCE)

        vals = {ix['Vertex']: ix['PageRank']
                for _, ix in pandas_vertices.iterrows()}

        self.assertItemsEqual(vals, nx_pagerank)


if __name__ == '__main__':
    unittest.main()
