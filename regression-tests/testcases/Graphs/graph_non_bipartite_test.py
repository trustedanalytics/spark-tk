##############################################################################
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
   Usage:  python2.7 graph_non_bipartite_test.py
   Construct a non-bipartite graph.

    Functionality tested:
        Specifically, build a graph in which edges connect same-class vertices,
        including triangles.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class GraphNonBipartiteTest(atk_test.ATKTestCase):

    def setUp(self):
        """Create target frame"""
        super(GraphNonBipartiteTest, self).setUp()

        dataset = "clustering_graph_edges.csv"
        schema = [('src', ia.int32),
                  ('dest', ia.int32),
                  ('xit_cost', ia.int32)]
        self.frame = frame_utils.build_frame(dataset, schema, self.prefix)

    def test_build_general_001(self):
        """
        Build a simple non-bipartite graph.
        Validate vertex and edge counts.
        Since Seamless graphs do not have any complex methods yet,
          there is little else to test here.
        :return: Exception if failed
        """

        expected_vcount = 6
        expected_ecount = 7

        print self.frame.inspect()
        graph = ia.Graph()
        graph.define_vertex_type('State')
        graph.define_edge_type('Transition', 'State', 'State', directed=True)

        print "Add vertices and edges"
        graph.vertices['State'].add_vertices(self.frame, 'src')
        graph.vertices['State'].add_vertices(self.frame, 'dest')
        graph.edges['Transition'].add_edges(self.frame, 'src',
                                            'dest',
                                            ['xit_cost'],
                                            create_missing_vertices=True)

        print graph.vertex_count, "vertices"
        print graph.edge_count, "edges"
        self.assertEqual(expected_vcount, graph.vertex_count)
        self.assertEqual(expected_ecount, graph.edge_count)


if __name__ == "__main__":
    unittest.main()
