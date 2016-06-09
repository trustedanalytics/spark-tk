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
##############################################################################
"""
usage:
python2.7 graphx_connected_components_test.py

Tests the connected_components function of graphx, as exposed by ATK. Values
are checked against networkx's connected_components() function.
"""
__author__ = 'bharadwa'

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest
import requests

import trustedanalytics as ia
import networkx as nx

from qalib import frame_utils
from qalib import atk_test


class GraphxConnectedComponents(atk_test.ATKTestCase):

    @classmethod
    def setUpClass(cls):
        """For performance reasons this is done here"""
        super(GraphxConnectedComponents, cls).setUpClass()

        graph_data = "cliques_10_10.csv"
        schema = [('from_node', str), ('to_node', str),
                  ('max_k', ia.int64), ('cc', ia.int64)]

        cls.frame = frame_utils.build_frame(graph_data, schema)

        appender = cls.frame.copy()
        appender.drop_columns(["from_node"])
        appender.rename_columns({"to_node": "from_node"})

        # Build the Parquet graph
        # get a name and create a Parquet Graph
        pgraph = ia.Graph()

        # define vertex and edge types of the graph
        pgraph.define_vertex_type('node')
        pgraph.define_edge_type('edge', 'node', 'node', directed=True)

        # Add vertices and edges to the graph from the frame
        pgraph.vertices['node'].add_vertices(
            cls.frame, cls.frame.from_node.name, ['max_k', 'cc'])
        pgraph.vertices['node'].add_vertices(
            appender, appender.from_node.name, ['max_k', 'cc'])

        pgraph.edges['edge'].add_edges(
            cls.frame, cls.frame.from_node.name,
            cls.frame.to_node.name, ['cc'])

        cls.pgraph_prime = pgraph

    def setUp(self):
        """Build frames to be exercised."""
        super(GraphxConnectedComponents, self).setUp()
        self.pgraph = self.pgraph_prime.copy()

    def test_null_property_name(self):
        """Test errors when output is set to empty"""

        print "Error is expected:"
        with self.assertRaises(ia.rest.command.CommandServerError):
            self.pgraph.graphx_connected_components("")

    def test_connected_component(self):
        """ Tests the graphx connected components in ATK
        """
        components = self.pgraph.graphx_connected_components("Con_Com")
        com_frame = components["node"].download(components['node'].row_count)
        con_com = com_frame.groupby("Con_Com")

        self._verify_connected_list(con_com)

    def _verify_connected_list(self, con_com):
        conn_list = []
        for group, values in con_com:
            conn_list.append(list(values["from_node"]))

        edge_list = self.frame.take(
            n=self.frame.row_count, columns=['from_node', 'to_node'])

        G = nx.Graph()
        G.add_edges_from(edge_list)
        networkx_result = map(sorted, nx.connected_components(G))
        # Sorting each individual component:

        self.assertEqual(sorted(map(sorted, conn_list)),
                         sorted(networkx_result))


if __name__ == '__main__':
    unittest.main()
