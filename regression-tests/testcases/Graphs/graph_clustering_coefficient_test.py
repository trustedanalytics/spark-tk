# #############################################################################
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
Usage: python2.7 graph_clustering_coefficient.py
Tests the clustering coefficient functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class GraphClusteringCoefficient(atk_test.ATKTestCase):

    def setUp(self):
        """Build frames and graphs to be tested."""
        super(GraphClusteringCoefficient, self).setUp()

        schema_node = [("node_name", str),
                       ("labeled_coeff", ia.float64),
                       ("unlabeled_coeff", ia.float64)]

        schema_undirected_label = [("node_from", str),
                                   ("node_to", str),
                                   ("labeled", str)]

        schema_undirected_unlabeled = [("node_from", str),
                                       ("node_to", str)]

        # Build the frames for the graph
        node_frame = frame_utils.build_frame(
            "clustering_graph_nodes.csv", schema_node, self.prefix)
        main_edge_frame = frame_utils.build_frame(
            "clustering_graph_edges.csv", schema_undirected_label, self.prefix)
        second_edge_frame = frame_utils.build_frame(
            "cluster_coefficient_secondary_edges.csv",
            schema_undirected_unlabeled,
            self.prefix)

        # Build the graph
        graph_name = common_utils.get_a_name(self.prefix)
        graph = ia.Graph(name=graph_name)

        # add the frames to the graph
        graph.define_vertex_type("primary")

        graph.vertices['primary'].add_vertices(node_frame,
                                               "node_name",
                                               ["labeled_coeff",
                                                "unlabeled_coeff"])

        graph.define_edge_type("main_set", "primary", "primary",
                               directed=False)

        graph.define_edge_type("second_set", "primary", "primary",
                               directed=False)

        graph.edges['main_set'].add_edges(main_edge_frame, "node_from",
                                          "node_to")
        graph.edges['second_set'].add_edges(second_edge_frame, "node_from",
                                            "node_to")
        # save the parquet graph
        self.p_graph = graph

    def test_clustering_coefficient_parquet_output(self):
        """ test the format of the output of function
            for parquet graph
        """
        result = self.p_graph.clustering_coefficient("cluster_prop")
        self.assertAlmostEqual(result.global_clustering_coefficient, 6.0/15.0)

        results = result.frame.take(result.frame.row_count)

        schema = result.frame.schema

        # The local coefficient was calculated by hand and added as an
        # attribute to the nodes.
        for node in results:
            self.assertAlmostEqual(
                node[map(lambda (x, y): x, schema).index('unlabeled_coeff')],
                node[map(lambda (x, y): x, schema).index('cluster_prop')],
                delta=.001)


if __name__ == "__main__":
    unittest.main()
