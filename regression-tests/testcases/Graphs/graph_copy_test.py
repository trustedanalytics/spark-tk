##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 graph_copy.py
   Test graph copy
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "2015.02.04"


# Functionality tested:
#   copy empty graph
#   copy graph with properties
#   drop graph and/or copy

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class GraphCopyTest(atk_test.ATKTestCase):

    def setUp(self):
        """ Standard: build the movie graph as in our product example. """
        super(GraphCopyTest, self).setUp()

        datafile = "movie.csv"

        # create basic movie graph

        self.schema = [('user', ia.int32),
                       ('vertexType', str),
                       ('movie', ia.int32),
                       ('rating', str),
                       ('splits', str)]

        self.frame = frame_utils.build_frame(
            datafile, self.schema, self.prefix, skip_header_lines=1)

        self.graph = ia.Graph(name=common_utils.get_a_name(self.prefix))

        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user',
                                    'movie', directed=True)

        # add vertices & edges, with properties
        self.graph.vertices['user'].add_vertices(self.frame, 'user')
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie')
        self.graph.edges['rating'].add_edges(self.frame, 'user', 'movie',
                                             create_missing_vertices=False)

        # Check graph construction
        self.assertEqual(self.graph.vertices['user'].row_count, 597)
        self.assertEqual(self.graph.vertices['movie'].row_count, 597)
        self.assertEqual(self.graph.edges['rating'].row_count, 597 * 2)

    def test_graph_copy_empty(self):
        """
        Copy null graph.
        Validate copy properties.
        """
        graf_name = common_utils.get_a_name(self.prefix)
        graf = ia.Graph(name=graf_name)
        # print "original empty graph:", type(graf), "\n", graf
        copy_name = common_utils.get_a_name(self.prefix)
        copy = graf.copy(name=copy_name)

        self.assertEqual(copy.name, copy_name)
        # Compare print images to determine equality.
        self.assertEqual(copy.vertices.__repr__(), "")
        self.assertEqual(copy.edges.__repr__(), "")

    def test_graph_status(self):
        """Test graph status works correctly"""
        self.assertEqual(self.graph.status, "ACTIVE")
        ia.drop_graphs(self.graph)
        self.assertEqual(self.graph.status, "DROPPED")

    def test_graph_last_read_date(self):
        """Test graph last read date gets updated properly"""
        old_date = self.graph.last_read_date
        print self.graph.edges['rating'].inspect()
        new_date = self.graph.last_read_date
        self.assertNotEqual(old_date, new_date)

    def test_get_graph(self):
        """Test graph get retrieves appropriate graph """

        copy = ia.get_graph(self.graph.name)

        # Compare copy to original.
        # Compare print images to determine equality.
        # There are 2 vertex frames; they can come in either order.
        # self.assertEqual(copy.vertices.__repr__(),
        #                  self.graph.vertices.__repr__())
        copy_repr = copy.vertices.__repr__()
        self.assertIn("movie :", copy_repr)
        self.assertIn("user :", copy_repr)

        self.assertEqual(self.graph.vertices['user'].row_count,
                         copy.vertices['user'].row_count)
        self.assertEqual(self.graph.vertices['movie'].row_count,
                         copy.vertices['movie'].row_count)
        self.assertEqual(self.graph.edges['rating'].row_count,
                         copy.edges['rating'].row_count)
        self.assertEqual(self.graph.edges['rating'].schema,
                         copy.edges['rating'].schema)

    def test_graph_copy_drop_integrity(self):
        """
        Copy canonical test graph.
        Drop copy does not affect original.
        Drop original does not affect copy.
        """
        graf_name = self.graph.name
        copy_name = common_utils.get_a_name(self.prefix)
        copy = self.graph.copy(copy_name)
        self.assertIn(copy_name, ia.get_graph_names())

        # Drop copy does not affect original.
        ia.drop_graphs(copy)
        self.assertIn(graf_name, ia.get_graph_names())
        self.assertNotIn(copy_name, ia.get_graph_names())

        # Drop original does not affect copy.
        copy_name = common_utils.get_a_name(self.prefix)
        copy = self.graph.copy(copy_name)
        ia.drop_graphs(self.graph)
        self.assertNotIn(graf_name, ia.get_graph_names())
        self.assertIn(copy_name, ia.get_graph_names())
        self.assertEqual(copy.name, copy_name)  # copy's attribute is active

    def test_graph_copy_empty_name(self):
        """Copying with an empty name should error"""
        # print "Simple copy, new name is empty"
        self.assertRaises(ia.rest.command.CommandServerError,
                          self.graph.copy, '')


if __name__ == "__main__":
    unittest.main()
