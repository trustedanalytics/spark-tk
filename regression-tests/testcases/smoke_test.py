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
    Usage:  python2.7 smoke_test.py

    Primary Smoke Test for trustedanalytics.
    Validate basic frame and graph build,
        plus a smattering of operations on each.
"""
__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart, Venky Bharadwaj"]
__version__ = "16.12.2014.001"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test


class SmokeTest(atk_test.ATKTestCase):

    def test_build(self):
        """
        Build a non-trivial TitanGraph from the 500-line movie data file.
        Exercise frame functions
            drop_rows
            filter
            add_columns
            drop_columns
            copy
            inspect
            take
            row_count (attribute)
            assign_sample
            cumulative_sum
        and graph functions
            alternating_least_squares
            drop_graphs
        """

        error_row_count = 0
        movie_row_count = 499
        score_row_count = 460
        edge_count = 455
        vertex_count = 397
        good_count = 11
        score_sum = 1692.0

        # Top 500 lines of the movie_data.csv file
        dataset = "movie_data_500.csv"
        schema = [("movie_id", ia.int32), ("movie_name", str),
                  ("user_id", ia.int32), ("score", str), ("language", str),
                  ("genre", str), ("rating", str), ("film_collections", str),
                  ("gross_revenue", str), ("directed_by", str),
                  ("produced_by", str)]
        self.frame = frame_utils.build_frame(
            dataset, schema, self.prefix, skip_header_lines=1)
        # print "original frame creation:\n", self.frame.inspect(10)
        print "movie_row_count", self.frame.row_count
        self.assertEqual(self.frame.row_count, movie_row_count)

        # Some of the rows in the frame do not have compatible data.
        # These rows are put into error_frames() and are excluded from
        #   the actual frame. For instance, some producer names contain
        #   a comma, so the system parses an additional column.
        err_frame = self.frame.get_error_frame()

        self.assertIsNone(err_frame)

        f2 = self.frame.copy()  # Copy to preserve a master copy

        print "Drop rows with no score\n"
        f2.drop_rows(lambda row: row.score == "")  # Drop rows with no score.
        print "Row count after drop", self.frame.row_count
        self.assertEqual(f2.row_count, score_row_count)
                         #"TRIB-4220: row count is zero")
        # print "score populations:", f2.row_count, |
        #     score_row_count, self.frame.row_count

        f2.drop_columns(['gross_revenue', 'film_collections'])
        print "dropped gross revenue\n", f2.inspect()
        self.assertNotIn('gross_revenue', f2.column_names)
        self.assertNotIn('film_collections', f2.column_names)

        # Demonstrate data with "string based" rating
        f3 = f2.copy()
        print "Filter 'good' movies"
        f3.filter(lambda row: row.score == 'good')
        print f3.inspect()
        print f3.row_count, "'good' rows"
        self.assertEqual(f3.row_count, good_count)

        def normalize_score(row):
            score_table = {'terrible': 1.0,
                           'bad': 2.0,
                           'not good': 3.0,
                           'good': 4.0,
                           'very good': 5.0
                           }

            user_eval = row['score']
            if user_eval in score_table.keys():
                new_score = score_table[user_eval]
            elif isinstance(user_eval, (int, float, long)):
                if float(user_eval) > 5.0:
                    new_score = float(user_eval)/2
                else:
                    new_score = float(user_eval)
            else:
                # not a rating phrase or number; return whatever we found
                new_score = user_eval
            return new_score

        # Based on the normalize_score() function in ia_utils.py,
        # we can add a column to the frame where the scoring is consistent.
        print "Add normalized score column"
        f2.add_columns(normalize_score, ('movie_score', ia.float64))
        self.assertIn('movie_score', f2.column_names)
        f2.drop_columns('score')
        self.assertNotIn('score', f2.column_names)

        # Assign sample in training, test and validation.
        f2.assign_sample([0.6, 0.2, 0.2], ['TR', 'VA', 'TE'], "splits")

        print "Test a whole-column function"
        f2.cumulative_sum("movie_score")
        self.assertEqual(f2.take(movie_row_count)[-1][-1], score_sum)

        print "Create a graph"
        graph_name = common_utils.get_a_name(self.prefix)
        movie_graph = ia.Graph(graph_name)
        movie_graph.define_vertex_type("movie")
        movie_graph.vertices["movie"].add_vertices(
            f2, "movie_id", ["movie_name", "language"])

        movie_graph.define_vertex_type("user")
        movie_graph.vertices["user"].add_vertices(
            f2, "user_id")

        movie_graph.define_edge_type("score", "user", "movie")
        movie_graph.edges["score"].add_edges(f2, "user_id", "movie_id")
        graph = movie_graph

        # Disable this check: Titan graph build is asynchronous.
        #   We don't know that the graph is ready to query.
        # edge_count_result = graph.query.gremlin("g.E.count()")
        # print "  Edge count", edge_count_result
        # self.assertEqual(edge_count_result["results"][0], edge_count*2)
        #
        # vertex_count_result = graph.query.gremlin("g.V.count()")
        # print vertex_count_result
        # self.assertEqual(vertex_count_result["results"][0], vertex_count)

        print "run clustering coefficient"
        cc_result = graph.clustering_coefficient()

        self.assertEqual(cc_result.global_clustering_coefficient, 0.0)

        ia.drop_graphs(graph.name)
        ia.drop_frames([self.frame, f2, f3])

    def test_join(self):
        """
        Exercise a simple join operation.
        """

        left_frame = frame_utils.build_frame("top100US_cities.csv",
                                             schema=[("City", str),
                                                     ("State", str),
                                                     ("Population", str),
                                                     ("cities_key", str)],
                                             prefix=self.prefix)

        right_frame = frame_utils.build_frame("100MoviesWithYear.csv",
                                              schema=[("movies_key", str),
                                                      ("Title", str),
                                                      ("year", ia.int32)],
                                              delimit='\t',
                                              prefix=self.prefix)

        print "rows: %d left, %d right" % (left_frame.row_count,
                                           right_frame.row_count)

        # print  left_frame.inspect()   # Only needed if debugging
        # print right_frame.inspect()   # Only needed if debugging

        left_key = "cities_key"    # Column name to join on in the left frame
        right_key = "movies_key"    # Column name to join on in the right frame

        # Count values/items on each side of join
        left_counts = left_frame.group_by(left_key, {left_key: ia.agg.count})
        right_counts = right_frame.group_by(
            right_key, {right_key: ia.agg.count})
        # Make reference dictionaries from frames
        left_numbers = dict(left_counts.take(left_counts.row_count))
        right_numbers = dict(right_counts.take(right_counts.row_count))

        def compute_rows():
            """
            Determines how many rows we should get based on type of join.
            In smoke_test, this is used only for an inner join;
              the outer-join facilities are disabled.
            """
            inner = 0    # Sets up counters used below
            # lefts_only = 0
            # rights_only = 0

            # Compute the product-sum of the common keys.
            # For every key in the left frame
            #   if it exists in the right frame
            #   add the product of the 2 to 'inner'.
            for key in left_numbers:
                if key in right_numbers:
                    inner += left_numbers[key] * right_numbers[key]
            return inner

        combo = left_frame.join(right_frame,
                                left_on=left_key,
                                right_on=right_key,
                                how="inner")
        print combo.row_count, "rows in join"
        self.assertEqual(combo.row_count, compute_rows())
        # print combo.inspect(2000)
        ia.drop_frames(combo)

        ia.drop_frames([left_frame, right_frame])
        ia.drop_frames([left_counts, right_counts])

    def test_seamless_build(self):
        """
        Build a non-trivial Seamless graph.
        """

        # Build a seamless movie graph

        datafile = "movie_tiny.csv"
        self.schema = [('user', ia.int32),
                       ('vertexType', str),
                       ('movie', ia.int32),
                       ('rating', str),
                       ('splits', str)]

        print("create big frame")
        self.frame = frame_utils.build_frame(
            datafile, self.schema, self.prefix, skip_header_lines=1)

        print("create graph")
        self.graph = ia.Graph()

        print("define vertices and edges")
        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user', 'movie', directed=True)

        # add vertices & edges, with properties
        print "add user vertices"
        self.graph.vertices['user'].add_vertices(self.frame, 'user')

        print "add movie vertices without properties"
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie')

        print "add edges without properties"
        self.graph.edges['rating'].add_edges(self.frame, 'user', 'movie',
                                             create_missing_vertices=False)

        # Check graph construction
        vpop = 13   # population of users, movies
        epop = 24   # population of edges
        self.assertEqual(self.graph.vertices['user'].row_count, vpop)
        self.assertEqual(self.graph.vertices['movie'].row_count, vpop)
        self.assertEqual(self.graph.edges['rating'].row_count, epop)

        ia.drop_graphs(self.graph)


if __name__ == "__main__":
    unittest.main()
