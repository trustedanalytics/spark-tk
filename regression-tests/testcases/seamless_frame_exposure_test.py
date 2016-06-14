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
   Usage:  python2.7 seamless_frame_exposure_test.py
   Test that Frame methods are properly exposed by Seamless Graph class
"""

__author__ = "WDW"
__credits__ = ["Prune Wickart"]
__version__ = "03.08.2015.001"

# Functionality tested:
#   copy()
#   take
#   inspect
#   drop_graph
#
#   filter
#   drop_rows
#   drop_duplicates
#   drop_columns
#   flatten_column
#   rename_columns

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import atk_test
from qalib import config


class GraphFrameExposeTest(atk_test.ATKTestCase):

    def setUp(self):
        super(GraphFrameExposeTest, self).setUp()

        movie_file = "movie.csv"
        # create basic movie graph
        print "define csv file"
        self.schema = [('user', ia.int32),
                       ('vertexType', str),
                       ('movie', ia.int32),
                       ('rating', str),
                       ('splits', str)]

        print "create big frame"
        self.frame = frame_utils.build_frame(
            movie_file, self.schema, self.prefix, skip_header_lines=1)

        print "create graph"
        self.graph = ia.Graph()

        print "define vertices and edges"
        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user', 'movie', directed=True)

        print "add user vertices"
        self.graph.vertices['user'].add_vertices(self.frame, 'user')

        print "add movie vertices"
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie')

        print "add edges"
        self.graph.edges['rating'].add_edges(self.frame, 'user', 'movie',
                                             ['rating'],
                                             create_missing_vertices=False)

        # Check graph construction
        self.entry_count = 597
        self.assertEqual(self.graph.vertices['user'].row_count,
                         self.entry_count)
        self.assertEqual(self.graph.vertices['movie'].row_count,
                         self.entry_count)
        self.assertEqual(self.graph.edges['rating'].row_count,
                         self.entry_count*2)

    def test_seamless_expose_frame(self):
        """
        Passive positive tests: don't change the graph or frame population.
        take
        inspect
        add_vertices and edges without properties

        Final tests: drop frame & graph
        drop_frames
        drop_graphs
        """

        movie_frame = self.graph.vertices["movie"]
        user_frame = self.graph.vertices["user"]
        rating_frame = self.graph.edges["rating"]

        print "Check rows & columns in a full-sized 'take'"
        movie_take = movie_frame.take(movie_frame.row_count)
        # print movie_frame.inspect(10)
        self.assertEqual(len(movie_take[0]), len(movie_frame.column_names))

        print "Check rows & columns in a full-sized 'take'"
        rating_take = rating_frame.take(rating_frame.row_count)
        # print movie_frame.inspect(10)
        self.assertEqual(len(rating_take[0]), len(rating_frame.column_names))

        # print ("ratings:", rating_frame.row_count, "edges:",
        #        self.graph.edge_count)
        # print ("movies:", movie_frame.row_count, "users:",
        #        user_frame.row_count, "vertices:", self.graph.vertex_count)
        self.assertEqual(rating_frame.row_count, self.graph.edge_count)
        self.assertEqual(movie_frame.row_count + user_frame.row_count,
                         self.graph.vertex_count)

        print "Add vertex with no properties"
        dataset = config.data_location+"/typesTest3.csv"
        add_schema = [("col_A", ia.int32),
                      ("col_B", ia.int64),
                      ("col_C", ia.float32),
                      ("Double", ia.float64),
                      ("Text", str)]
        add_csv = ia.CsvFile(dataset, add_schema)
        add_frame = ia.Frame(add_csv)

        self.graph.define_vertex_type('no_property_v')
        self.graph.vertices['no_property_v'].add_vertices(add_frame, "col_A")
        # self.graph.vertices['no_property_v'].add_vertices(ia.Frame(),
        #                                                   'unused_space')
        print self.graph.vertices

        # TRIB-4069
        print "drop a vertex frame"
        name = movie_frame.name
        ia.drop_frames(movie_frame)
        self.assertNotIn(name, ia.get_frame_names())

        # TRIB-4069
        print "drop an edge frame"
        name = rating_frame.name
        ia.drop_frames(rating_frame)
        self.assertNotIn(name, ia.get_frame_names())

        print "drop the whole graph"
        name = self.graph.name
        ia.drop_graphs(self.graph)
        self.assertNotIn(name, ia.get_graph_names())

    def test_seamless_expose_vertex(self):
        """
        Active positive tests: add/drop graph, frame, or column; alter
        contents.
        Focus on vertex frame

        copy()
        filter
        drop_rows
        drop_duplicates
        drop_columns
        rename_columns
        """

        print "Frame copy is same size as original"
        movie_frame = self.graph.vertices["movie"]
        # print movie_frame.inspect(10)
        print "Movie frame has", movie_frame.row_count, "rows"
        print "Movie frame has", len(movie_frame.column_names), "cols"

        # TRIB-4083
        movie_copy = movie_frame.copy()
        self.assertEqual(movie_frame.column_names, movie_copy.column_names)
        self.assertEqual(movie_frame.row_count, movie_copy.row_count,
                         "row_count is 0:"
                         "https://jira01.devtools.intel.com/browse/TRIB-4220")

        print "drop_duplicates -- there are none"
        movie_vertex_count = movie_frame.row_count
        movie_frame.drop_duplicates()
        print "Movie frame has", movie_frame.row_count, "rows"
        self.assertEqual(movie_frame.row_count, movie_vertex_count)

        # In first implementation, there are 597 movies.
        #   Dropping all with an ID < -10,000 leaves 405 rows.
        print "Drop some rows"
        movie_vertex_count = movie_frame.row_count
        movie_frame.drop_rows(lambda row: row.movie < -10000)
        print "Movie frame has", movie_frame.row_count, "rows"
        self.assertLess(movie_frame.row_count, movie_vertex_count)

        # Dropping Vertex IDs <= 1700 leaves 233 rows
        print "Filter some rows"
        print "Before filter:\n", movie_frame.inspect(20)
        movie_vertex_count = movie_frame.row_count

        try:
            movie_frame.filter(lambda row: row._vid > 1700)
        except Exception as e:
            print "https://jira01.devtools.intel.com/browse/TRIB-4352"
            raise e

        print "After  filter:\n", movie_frame.inspect(20)
        print "Movie frame has", movie_frame.row_count, "rows"
        self.assertLess(movie_frame.row_count, movie_vertex_count,
                        "TRIB-4215: filter does not remove rows")

        print "Rename columns"
        movie_frame.rename_columns({"movie": "Movie_num"})
        self.assertIn("Movie_num", movie_frame.column_names)

    def test_expose_vertex_10(self):
        """
        Active positive tests for methods exposed in release 1.0
        Focus on vertex frame

        sort
        flatten
        get_err_frame
        """

        test_frame = self.graph.vertices["movie"]
        print test_frame.inspect(10)
        test_count = test_frame.row_count
        test_frame.sort("_vid")
        self.assertEqual(test_count, test_frame.row_count,
                         "Expected %d rows; found %d" %
                         (test_count, test_frame.row_count))

        self.assertIsNone(test_frame.get_error_frame())

        test_frame.flatten_columns("_label")
        self.assertEqual(test_count, test_frame.row_count,
                         "Expected %d rows; found %d" %
                         (test_count, test_frame.row_count))

    # def test_expose_edge_10(self):
        """
        Active positive tests for methods exposed in release 1.0
        Focus on edge frame

        sort
        flatten
        get_err_frame
        """

        test_frame = self.graph.edges["rating"]
        print test_frame.inspect(10)
        test_count = test_frame.row_count
        test_frame.sort("_eid")
        self.assertEqual(test_count, test_frame.row_count,
                         "Expected %d rows; found %d" %
                         (test_count, test_frame.row_count))

        self.assertIsNone(test_frame.get_error_frame())

        test_frame.flatten_columns("_label")
        self.assertEqual(test_count, test_frame.row_count,
                         "Expected %d rows; found %d" %
                         (test_count, test_frame.row_count))

    def test_expose_join(self):
        """ Test join operation for both vertex and edge frames """
        movie_frame = self.graph.vertices["movie"]
        rating_frame = self.graph.edges["rating"]
        ve_join = movie_frame.join(
            rating_frame, "_vid", "_dest_vid", how="left")
        print ve_join.inspect(10), '\nrow_count:', ve_join.row_count
        self.assertEqual(ve_join.row_count, self.entry_count * 2)

        ev_join = rating_frame.join(
            movie_frame, "_dest_vid", "_vid", how="left")
        print ev_join.inspect(10), '\nrow_count:', ev_join.row_count
        self.assertEqual(ev_join.row_count, self.entry_count * 2)

    def test_seamless_expose_edge(self):
        """
        Active positive tests: add/drop graph, frame, or column; alter
        contents.
        Focus on edge frame

        copy()
        filter
        drop_rows
        drop_duplicates
        drop_columns
        rename_columns
        """

        # Empirical counts from the data file
        edge_non_zero_rating_count = 1176
        edge_high_rating_count = 398

        print "Frame copy is same size as original"
        edge_frame = self.graph.edges["rating"]
        print edge_frame.inspect(10)
        print "Edge frame has", edge_frame.row_count, "rows"
        print "Edge frame has", len(edge_frame.column_names), "cols"

        # TRIB-4083
        edge_copy = edge_frame.copy()
        self.assertEqual(edge_frame.column_names, edge_copy.column_names)
        self.assertEqual(edge_frame.row_count, edge_copy.row_count,
                         "row_count is 0: "
                         "https://jira01.devtools.intel.com/browse/TRIB-4220")

        # TRIB-4076
        # Use the frame copy
        print "drop_duplicates -- only one of each rating should remain"
        # print "BEFORE:\n", edge_frame.inspect(20)
        edge_copy.drop_duplicates("rating")
        # print "AFTER:\n", edge_copy.inspect(20)
        print "Edge frame has", edge_copy.row_count, "rows"
        self.assertEqual(edge_copy.row_count, 8)

        # In first implementation, there are 1194 (597*2) rating edges.
        #   Dropping all 0 ratings leaves 1176 rows.
        print "Drop some rows"
        edge_frame.drop_rows(lambda row: row.rating == '0')
        print "Edge frame has", edge_frame.row_count, "rows"
        self.assertEqual(edge_non_zero_rating_count, edge_frame.row_count)

        # Dropping all low ratings leaves 398 rows
        print "Filter some rows"
        edge_frame.filter(lambda row: row.rating >= '4')
        print "Edge frame has", edge_frame.row_count, "rows"
        self.assertEqual(edge_high_rating_count, edge_frame.row_count)

        print "Rename columns"
        edge_frame.rename_columns({"rating": "star_level"})
        self.assertIn("star_level", edge_frame.column_names)

        print "Now we get destructive: drop a column"
        self.assertIn("star_level", edge_frame.column_names)
        edge_frame.drop_columns("star_level")
        print "Edge frame has", len(edge_frame.column_names), "cols"
        self.assertNotIn("star_level", edge_frame.column_names)


if __name__ == "__main__":
    unittest.main()
