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
   Usage:  python2.7 seamless_alter_test.py -r seamless_alter_test.json
                    -l seamless_alter_test.log
   Test operations of trivial and altered graphs
"""
__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "18.11.2014.001"


# Functionality tested:
#   V|E with properties; check schema
#   V|E columns added, dropped; check schema, values
#   Cannot alter system cols: _vid, _eid, _label, _src_id, _dest_id, et alia

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test


class GraphAlterTest(atk_test.ATKTestCase):

    def setUp(self):
        super(GraphAlterTest, self).setUp()

        datafile = "movie.csv"

        # create basic movie graph

        print "define csv file"
        self.schema = [('user', ia.int32),
                       ('vertexType', str),
                       ('movie', ia.int32),
                       ('rating', str),
                       ('splits', str)]

        print "create big frame"
        self.frame = frame_utils.build_frame(
            datafile, self.schema, self.prefix, skip_header_lines=1)

        print "create graph"
        self.graph = ia.Graph()

        print "define vertices and edges"
        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user',
                                    'movie', directed=True)

        # add vertices & edges, with properties
        print "add user vertices"
        self.graph.vertices['user'].add_vertices(self.frame, 'user')

        print "add movie vertices without properties"
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie')

        print "add edges without properties"
        self.graph.edges['rating'].add_edges(self.frame, 'user', 'movie',
                                             create_missing_vertices=False)

        # Check graph construction
        self.assertEqual(self.graph.vertices['user'].row_count, 597)
        self.assertEqual(self.graph.vertices['movie'].row_count, 597)
        self.assertEqual(self.graph.edges['rating'].row_count, 597 * 2)

    def test_seamless_add_prop(self):
        """
        Add vertex & edge with no properties;
          validate schema changes & data
        """

        v_orig = [v.schema[2][0] for v in self.graph.vertices]
        print "v_orig", v_orig
        e_orig = [e.schema[2][0] for e in self.graph.edges]
        print "e_orig", e_orig
        movie_frame = self.graph.vertices["movie"]
        user_frame = self.graph.vertices["user"]
        rating_frame = self.graph.edges["rating"]

        movie_head = movie_frame.inspect(0)
        user_head = user_frame.inspect(0)
        rating_head = rating_frame.inspect(0)
        print "\nmovie:", movie_head
        print "\nuser:", user_head
        print "\rating:", rating_head

        print "Add vertex with properties"
        dataset = "typesTest3.csv"
        add_schema = [("col_A", ia.int32),
                      ("col_B", ia.int64),
                      ("col_C", ia.float32),
                      ("Double", ia.float64),
                      ("Text", str)]
        add_frame = frame_utils.build_frame(
            dataset, add_schema, self.prefix, skip_header_lines=1)

        v_add_frame = "v_dummy"
        v_add_name = "col_A"
        self.graph.define_vertex_type(v_add_frame)
        self.graph.vertices[v_add_frame].add_vertices(add_frame,
                                                      v_add_name, "Text")
        v_later = [v.schema[2][0] for v in self.graph.vertices]
        print v_later
        self.assertNotIn(v_add_name, v_orig)
        self.assertIn(v_add_name, v_later)

        e_add_frame = "e_dummy"
        e_add_name = "Double"
        self.graph.define_edge_type(e_add_frame, 'v_dummy', 'user',
                                    directed=True)
        self.graph.edges[e_add_frame].add_edges(add_frame, e_add_name,
                                                v_add_name)
        e_later = [e.schema[2][0] for e in self.graph.edges]
        print e_later
        print self.graph.edges
        self.assertNotIn(e_add_name, e_orig)
        # self.assertIn(e_add_name, e_later)

    def test_seamless_delete_prop(self):
        # Delete vertex & edge with properties;
        #   validate schema changes & data

        v_orig = [v.schema[2][0] for v in self.graph.vertices]
        print "v_orig", v_orig
        e_orig = [e.schema[2][0] for e in self.graph.edges]
        print "e_orig", e_orig
        # movie_frame = self.graph.vertices["movie"]
        user_frame = self.graph.vertices["user"]
        # rating_frame = self.graph.edges["rating"]

        # movie_head = movie_frame.inspect(0)
        # user_head = user_frame.inspect(0)
        # rating_head = rating_frame.inspect(0)

        # Delete a property; validate remaining columns
        print "Vertices object type:", type(self.graph.vertices)
        print user_frame.name in self.graph.vertices
        print user_frame in self.graph.vertices
        print "TRIB 4069: drop_frames not exposed for vertex frames"
        ia.drop_frames(user_frame)
        print self.graph.vertices

    def test_seamless_drop_rows(self):
        # drop duplicates; check resulting row count
        pass


if __name__ == "__main__":
    unittest.main()
