##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
# ############################################################################
"""
usage:
python2.7 graphx_page_rank_test.py

Tests PageRank exposed in ATK from graphx. Validated against networkx's
implementation.
"""
__author__ = 'bharadwa'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import requests
import operator

import trustedanalytics as ia
import networkx as nx

from qalib import frame_utils
from qalib import common_utils as cu
from qalib import atk_test


class GraphxPageRank(atk_test.ATKTestCase):

    def setUp(self):
        """Creates both frame and graph is used by multiple tests."""
        super(GraphxPageRank, self).setUp()

        self.MAX_ITERATIONS = 20
        self.CONVERGENCE_TOLERANCE = 0.001
        self.TOP_N_RANKS = 20

        graph_data = "twitter_edges_small.csv"
        schema = [("followed", ia.int32), ("follows", ia.int32)]

        self.frame = frame_utils.build_frame(
            graph_data, schema, self.prefix, skip_header_lines=1)

        graph_name = cu.get_a_name(self.prefix)
        self.graph = ia.Graph(graph_name)

        self.graph.define_vertex_type("node")
        self.graph.vertices["node"].add_vertices(self.frame, "follows")
        self.graph.vertices["node"].add_vertices(self.frame, "followed")

        self.graph.define_edge_type("e1", "node", "node", directed=True)
        self.graph.edges["e1"].add_edges(self.frame, "follows", "followed")

    def _get_dict_from_networkx(self):
        """Creates a networkx graph and then computes pagerank on the graph.

        It uses an edge list for graph creation obtained from the take()
        function in ATK.

        The format of the return is shown below

        {1: 9.108956222284042e-05,
         3: 0.0008779429596023876,
         5: 4.8390254821794703e-05,
         6: 6.333877181924667e-05}

        Returns
        ------
        dictionary<Node,Float>
            A dictionary containing nodes as keys and their pagerank
            value as values

        """
        G = nx.Graph()
        G.add_edges_from(self.edge_list)
        return nx.pagerank(G, max_iter=self.MAX_ITERATIONS,
                           tol=self.CONVERGENCE_TOLERANCE)

    def test_null_property_name(self):
        """ Negative test to verify property value being null errors"""
        print "Null property name"
        self.assertRaisesRegexp((requests.exceptions.HTTPError,
                                 ia.rest.command.CommandServerError),
                                "Output property label must be provided",
                                self.graph.graphx_pagerank,
                                output_property="")

    def test_string_in_max_iterations(self):
        """Negative test to verify integer type for max_iterations."""
        print "String where number expected in max iterations"
        self.assertRaisesRegexp((requests.exceptions.HTTPError,
                                 ia.rest.command.CommandServerError),
                                "Expected Int",
                                self.graph.graphx_pagerank,
                                output_property="PR",
                                max_iterations="20")

    def test_string_convergence_tol(self):
        """Negative test to verify convergence_tolerance type is checked."""
        print "String where number expected in convergence"
        self.assertRaisesRegexp((requests.exceptions.HTTPError,
                                 ia.rest.command.CommandServerError),
                                "Expected Double",
                                self.graph.graphx_pagerank,
                                output_property="PR",
                                max_iterations=0,
                                convergence_tolerance="0.001")

    def _get_top_n_nodes(self, n, dictionary):
        """Sorts a dictionary on its page rank value

        Parameters
        ----------
        n : int
            The top n number of values that are to be returned by the function
        dictionary : dict<Node -> Float>
            The dictionary that needs to be sorted

        Returns
        -------
        list<Nodes>
            The list of top n keys(node) of dictionary based on
            their decreasing value(pagerank).
        """
        sorted_dict = sorted(dictionary.items(),
                             key=operator.itemgetter(1), reverse=True)
        return [x[0] for x in sorted_dict[:(n+1)]]

    def test_page_rank_networkx_comparison_parquet(self):
        """ Tests the graphx pagerank exposed in ATK on parquet graph

        Compares the results obtained from networkx pagerank() function.
        Networkx creates graph using edge list which is generated using
        take on the frame.
        Comparing top N ranked values and not all to same time. I explicitly
        supplied the default values for max_iterations and
        convergence_tolerance to make obvious comparisons with networkx values.
        """

        result = self.graph.graphx_pagerank(
            output_property="PR",
            max_iterations=self.MAX_ITERATIONS,
            convergence_tolerance=self.CONVERGENCE_TOLERANCE)

        vertices = result['vertex_dictionary']['node']
        pandas_vertices = vertices.download(vertices.row_count)

        # Creates a dictionary of triangle count per node,
        # so as to match networkx output:
        dictionary_of_triangle_count = \
            {i['follows']: (i['PR']) for (_, i) in pandas_vertices.iterrows()}
        row_count = self.frame.row_count
        # checking if frame has any rows or not.
        self.assertGreater(row_count, 0)
        self.edge_list = self.frame.take(
            n=row_count, columns=['followed', 'follows'])

        print "take for edge list done"
        pagerank_counts_from_networkx = self._get_dict_from_networkx()

        print "networkx page rank calculated"
        ATK_top_n_ranked_node = self._get_top_n_nodes(
            self.TOP_N_RANKS, dictionary_of_triangle_count)
        networkx_top_n_ranked_node = self._get_top_n_nodes(
            self.TOP_N_RANKS, pagerank_counts_from_networkx)

        print "ATK_ ranks ", ATK_top_n_ranked_node
        print "networkx ranks", networkx_top_n_ranked_node
        self.assertEqual(ATK_top_n_ranked_node, networkx_top_n_ranked_node)


if __name__ == '__main__':
    unittest.main()
