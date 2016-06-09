#############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014, 2015 Intel Corporation All Rights Reserved.
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
##############################################################################
"""
usage:
python2.7 graphx_triangle_count.py

Tests the triangle count function exposed by ATK from graphx. Checked
against the networkx implementation

"""

# TODO BREAK Out the empty graph name test
__author__ = 'bharadwa'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia
import networkx as nx

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class GraphxTriangleCount(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames and graphs to exercise"""
        super(GraphxTriangleCount, self).setUp()
        graph_data = "cliques_10_10.csv"
        schema = [('from_node', str),
                  ('to_node', str),
                  ('max_k', ia.int64),
                  ('cc', ia.int64)]

        self.frame = frame_utils.build_frame(graph_data, schema, self.prefix)

        # build directed graph
        name_directed = common_utils.get_a_name(self.prefix)
        self.directed_graph = ia.Graph(name_directed)

        self.directed_graph.define_vertex_type("node")
        self.directed_graph.vertices["node"].add_vertices(
            self.frame, "from_node", ["max_k", "cc"])

        self.directed_graph.vertices["node"].add_vertices(
            self.frame, "to_node", ["max_k", "cc"])

        self.directed_graph.define_edge_type(
            "edge", "node", "node", directed=True)
        self.directed_graph.edges["edge"].add_edges(
            self.frame, "from_node", "to_node")

        # build undirected graph
        name_undirected = common_utils.get_a_name(self.prefix)
        self.undirected_graph = ia.Graph(name_undirected)

        self.undirected_graph.define_vertex_type("node")
        self.undirected_graph.vertices["node"].add_vertices(
            self.frame, "from_node", ["max_k", "cc"])

        self.undirected_graph.vertices["node"].add_vertices(
            self.frame, "to_node", ["max_k", "cc"])

        self.undirected_graph.define_edge_type(
            "edge", "node", "node", directed=False)
        self.undirected_graph.edges["edge"].add_edges(
            self.frame, "from_node", "to_node")

    def test_null_property_name(self):
        """ Negative test case for triangle count property value set to null"""
        print "Error is expected:"

        self.assertRaisesRegexp(
            (ia.rest.command.CommandServerError),
            "Output property label must be provided",
            self.directed_graph.graphx_triangle_count,
            output_property="")

    def test_triangle_counts_directed_parquet(self):
        """Tests the graphx triangle count in ATK on parquet graph.

        Compares the results obtained from networkx triangles() function.
        A take on frame is used for generating edge list to be used by
        networkx for graph creation.
        """
        result = self.directed_graph.graphx_triangle_count(
            output_property="triangle")
        self._verify_triangle_graph(result['node'])

    def test_triangle_counts_undirected_parquet(self):
        """Tests the graphx triangle count in ATK on parquet graph.

        Compares the results obtained from networkx triangles() function.
        A take on frame is used for generating edge list to be used by
        networkx for graph creation.
        """
        result = self.undirected_graph.graphx_triangle_count(
            output_property="triangle")
        self._verify_triangle_graph(result['node'])

    def _verify_triangle_graph(self, frame_result):
        """Verify triangle graph against networkx's implementation
        of triangle count."""

        triangles = frame_result.download(frame_result.row_count)
        # Create a dictionary of triangle count per node:
        dictionary_of_triangle_count = {i['from_node']: (i['triangle'])
                                        for (_, i) in triangles.iterrows()}

        edge_list = self.frame.take(
            n=self.frame.row_count, columns=['from_node', 'to_node'])

        # build the network x result
        G = nx.Graph()
        G.add_edges_from(edge_list)
        triangle_counts_from_networkx = nx.triangles(G)

        self.assertEqual(dictionary_of_triangle_count,
                         triangle_counts_from_networkx)

if __name__ == '__main__':
    unittest.main()
