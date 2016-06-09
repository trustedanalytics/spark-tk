##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 - 2015 Intel Corporation All Rights Reserved.
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
   Usage:  python2.7 graph_vertex_frame_test.py
   Test that Frame statistic methods are properly exposed by Seamless Graph
   class
"""
__author__ = "WDW, KH"
__credits__ = ["Prune Wickart", "Karen Herrold"]
__version__ = "2015.02.17"


# Functionality tested:
#  assign_sample
#  bin_column
#  bin_column_equal_depth
#  bin_column_equal_width
#  classification_metrics
#  column_median
#  column_mode
#  column_summary_statistics
#  cumulative_percent
#  ecdf
#  entropy
#  group_by
#  quantiles
#  tally
#  tally_percent
#  top_k

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import json
import unittest

import trustedanalytics as ia

from qalib import frame_utils
from qalib import common_utils
from qalib import atk_test
from qalib import config as cfg


class GraphVertexFrameTest(atk_test.ATKTestCase):

    def setUp(self):
        # Call parent set-up.
        super(GraphVertexFrameTest, self).setUp()
        self.location = cfg.export_location

        self.data_csv_vertex = "ex_csv_v"
        self.data_json_vertex = "ex_json_v"
        self.schema_export_vertex = [("_vid", ia.int64),
                                     ("_label", str),
                                     ("movie", ia.int32)]

        # Create basic movie graph.
        datafile = "movie_small.csv"

        # Define csv file.
        self.schema = [('movie', ia.int32),
                       ('vertexType', str),
                       ('user', ia.int32),
                       ('rating', ia.int32),
                       ('splits', str),
                       ('percent', ia.int32),
                       ('predict', ia.int32),
                       ('weight', ia.int32),
                       ('age', ia.int32)]

        # Create frame.
        self.frame = frame_utils.build_frame(
            datafile, self.schema, skip_header_lines=1, prefix=self.prefix)

        # Create graph
        self.graph = ia.Graph()
        self.graph.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Define vertices and edges
        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user', 'movie', directed=True)

        # Add user vertices.  These would normally belong to the edge but I've
        # moved them to 'user' to provide data for some of the tests.  Should
        # not matter as long as they only exist in one place.
        self.graph.vertices['user'].add_vertices(self.frame,
                                                 'user',
                                                 ['age',
                                                  'percent',
                                                  'predict',
                                                  'rating',
                                                  'weight'])

        # Add movie vertices
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie', [])

        #  Add edges.
        self.graph.edges['rating'].add_edges(self.frame,
                                             'user',
                                             'movie',
                                             [],
                                             create_missing_vertices=False)

        # Check graph construction (constants determined using Excel)
        self.entry_count = 597  # Rows in data file
        self.user_node_count = 587
        self.movie_node_count = 10
        self.node_count = self.user_node_count + self.movie_node_count
        self.edge_count = 597

        self.assertEqual(self.user_node_count,
                         self.graph.vertices['user'].row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.user_node_count,
                                 self.graph.vertices['user'].row_count))
        self.assertEqual(self.movie_node_count,
                         self.graph.vertices['movie'].row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.movie_node_count,
                                 self.graph.vertices['movie'].row_count))
        self.assertEqual(self.edge_count,
                         self.graph.edges['rating'].row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.edge_count,
                                 self.graph.edges['rating'].row_count))
        self.assertEqual(self.edge_count,
                         self.graph.edge_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.edge_count, self.graph.edge_count))
        self.assertEqual(self.node_count,
                         self.graph.vertex_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.node_count, self.graph.vertex_count))

    def test_vertex_frame_assign_sample(self):
        """
        Happy path test for assign_sample for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Test exposure only; no validation of results.
        old_size = len(user_frame.schema)
        user_frame.assign_sample([0.3, 0.3, 0.4],
                                 ['train', 'test', 'validate'])
        self.assertEqual(old_size + 1, len(user_frame.schema))
        self.assertIn('sample_bin', user_frame.column_names)

    def test_vertex_frame_bin_column(self):
        """
        Happy path test for bin_column for vertex frames.
        """

        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        bin_return = user_frame.bin_column('age',
                                           [0.0, 25.0, 50.0, 75.0, 100.0])

        # Confirm there is no longer a return type issue
        self.assertIsNone(bin_return)

        user_frame.sort('age')
        bin_take = user_frame.take(user_frame.row_count,
                                   columns=['age', 'age_binned'])

        # Check a couple of the bin boundaries (determined by examination).
        self.assertEqual(0,
                         bin_take[30][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[30][1]))
        self.assertEqual(1,
                         bin_take[31][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[31][1]))
        self.assertEqual(2,
                         bin_take[-131][1],
                         'Expected: {0}, Actual: {1}'
                         .format(2, bin_take[-131][1]))
        self.assertEqual(3,
                         bin_take[-130][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-130][1]))

    def test_vertex_frame_bin_column_equal_depth(self):
        """
        Happy path test for bin_column_equal_depth for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        user_frame.sort('age')

        cutoff_points = user_frame.bin_column_equal_depth('age', 5)
        cutoff_expect = [1.0, 37.0, 53.0, 66.0, 77.0, 99.0]
        self.assertEqual(cutoff_expect,
                         cutoff_points,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoff_expect, cutoff_points))

        user_frame.sort('age')
        bin_take = user_frame.take(user_frame.row_count,
                                   columns=['age', 'age_binned'])

        # Check a couple of the bin boundaries (determined by examination).
        self.assertEqual(0,
                         bin_take[116][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[116][1]))
        self.assertEqual(0,
                         bin_take[117][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[117][1]))
        self.assertEqual(1,
                         bin_take[118][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[118][1]))
        self.assertEqual(3,
                         bin_take[-116][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-116][1]))
        self.assertEqual(4,
                         bin_take[-115][1],
                         'Expected: {0}, Actual: {1}'
                         .format(4, bin_take[-115][1]))

    def test_vertex_frame_bin_column_equal_width(self):
        """
        Happy path test for bin_column_equal_width for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # bin_return is the list of cutoff points
        cutoff_points = user_frame.bin_column_equal_width('age', 5)
        cutoff_expect = [1.0, 20.6, 40.2, 59.8, 79.4, 99.0]
        for i, j in zip(cutoff_points, cutoff_expect):
            self.assertAlmostEqual(
                i, j, msg='Expected: {0}, Actual: {1}'.format(i, j))

        user_frame.sort('age')
        bin_take = user_frame.take(user_frame.row_count,
                                   columns=['age', 'age_binned'])

        # Check a couple of the bin boundaries (determined by examination).
        self.assertEqual(0,
                         bin_take[22][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[22][1]))
        self.assertEqual(1,
                         bin_take[23][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[23][1]))
        self.assertEqual(3,
                         bin_take[-85][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-85][1]))
        self.assertEqual(4,
                         bin_take[-84][1],
                         'Expected: {0}, Actual: {1}'
                         .format(4, bin_take[-84][1]))

    def test_vertex_frame_classification_metrics(self):
        """
        Happy path test for classification_metrics for vertex frames.
        """

        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        def vcol_func(row):
            predict_wt = 1.5
            forecast = int(round(
                (row.percent // 18 + predict_wt * row.predict) /
                (1 + predict_wt)))

            influence = row.weight + 1
            return forecast, influence

        # Add 2 columns at once
        old_size = len(user_frame.schema)
        user_frame.add_columns(vcol_func, schema=[('forecast', ia.int32),
                                                  ('influence', ia.int32)])
        self.assertEqual(old_size + 2, len(user_frame.schema))
        self.assertIn('forecast', user_frame.column_names)
        self.assertIn('influence', user_frame.column_names)

        # Test classification_metrics.
        cm = user_frame.classification_metrics('predict', 'forecast')

        accuracy_expected = 0.543441226576
        self.assertAlmostEqual(accuracy_expected,
                               cm.accuracy,
                               places=12,
                               msg='Expected: {0}, Actual: {1}'
                               .format(accuracy_expected, cm.accuracy))

        recall_expected = accuracy_expected
        self.assertAlmostEqual(recall_expected,
                               cm.recall,
                               places=12,
                               msg='Expected: {0}, Actual: {1}'
                               .format(recall_expected, cm.recall))

        f_measure_expected = 0.537583857349
        self.assertAlmostEqual(f_measure_expected,
                               cm.f_measure,
                               places=12,
                               msg='Expected: {0}, Actual: {1}'
                               'Expected: {0}, Actual: {1}'
                               .format(f_measure_expected, cm.f_measure))

        precision_expected = 0.665335696496
        self.assertAlmostEqual(precision_expected,
                               cm.precision,
                               places=12,
                               msg='Expected: {0}, Actual: {1}'
                               .format(precision_expected, cm.precision))

        # confusion matrix should be not available for this table

    def test_vertex_frame_column_median(self):
        """
        Happy path test for column_median for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        median = user_frame.column_median('age')
        median_expected = 59    # calculated with Excel
        self.assertEqual(median_expected,
                         median,
                         'Expected: {0}, Actual: {1}'
                         .format(median_expected, median))

    def test_vertex_frame_column_mode(self):
        """
        Happy path test for column_mode passes for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        mode = user_frame.column_mode('age')
        weight_of_mode_expected = 15
        self.assertEqual(weight_of_mode_expected,
                         mode['weight_of_mode'],
                         'Expected: {0}, Actual: {1}'
                         .format(weight_of_mode_expected,
                                 mode['weight_of_mode']))

        mode_count_expected = 1
        self.assertEqual(mode_count_expected,
                         mode['mode_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(mode_count_expected, mode['mode_count']))

        modes_expected = [30]   # Calculated by Excel
        self.assertEqual(modes_expected,
                         mode['modes'],
                         'Expected: {0}, Actual: {1}'
                         .format(modes_expected, mode['modes']))

        total_weight_expected = self.user_node_count
        self.assertEqual(total_weight_expected,
                         mode['total_weight'],
                         'Expected: {0}, Actual: {1}'
                         .format(total_weight_expected, mode['total_weight']))

    def test_vertex_frame_column_summary_stats(self):
        """
        Happy path test for column_summary_statistics for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        stats = user_frame.column_summary_statistics('age')

        row_count_expected = self.user_node_count   # All rows are good
        self.assertEqual(row_count_expected,
                         stats['good_row_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(row_count_expected, stats['good_row_count']))

        non_pos_weight_count_expected = 0   # No negative values
        self.assertEqual(non_pos_weight_count_expected,
                         stats['non_positive_weight_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(non_pos_weight_count_expected,
                                 stats['non_positive_weight_count']))

        std_deviation_expected = 20.4489374591
        self.assertAlmostEqual(std_deviation_expected,
                               stats['standard_deviation'],
                               places=10,
                               msg='Expected: {0}, Actual: {1}'
                               .format(std_deviation_expected,
                                       stats['standard_deviation']))

        max_expected = 99.0     # Calculated using Excel
        self.assertEqual(max_expected,
                         stats['maximum'],
                         'Expected: {0}, Actual: {1}'
                         .format(max_expected, stats['maximum']))

        mean_confidence_upper_expected = 58.9813621142
        self.assertAlmostEqual(mean_confidence_upper_expected,
                               stats['mean_confidence_upper'],
                               places=10,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_confidence_upper_expected,
                                       stats['mean_confidence_upper']))

        mean_confidence_lower_expected = 55.6728116507
        self.assertAlmostEqual(mean_confidence_lower_expected,
                               stats['mean_confidence_lower'],
                               places=10,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_confidence_lower_expected,
                                       stats['mean_confidence_lower']))

        min_expected = 1    # Calculated using Excel
        self.assertEqual(min_expected,
                         stats['minimum'],
                         'Expected: {0}, Actual: {1}'
                         .format(min_expected, stats['minimum']))

        total_weight_expected = self.user_node_count
        self.assertEqual(total_weight_expected,
                         stats['total_weight'],
                         'Expected: {0}, Actual: {1}'
                         .format(total_weight_expected, stats['total_weight']))

        bad_row_count_expected = 0  # No bad rows
        self.assertEqual(bad_row_count_expected,
                         stats['bad_row_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(bad_row_count_expected,
                                 stats['bad_row_count']))

        variance_expected = 418.159043206
        self.assertAlmostEqual(variance_expected,
                               stats['variance'],
                               places=9,
                               msg='Expected: {0}, Actual: {1}'
                               .format(variance_expected, stats['variance']))

        geometric_mean_expected = 52.6823151616
        self.assertAlmostEqual(geometric_mean_expected,
                               stats['geometric_mean'],
                               places=10,
                               msg='Expected: {0}, Actual: {1}'
                               .format(geometric_mean_expected,
                                       stats['geometric_mean']))

        positive_weight_count_expected = self.user_node_count  # all values
        self.assertEqual(positive_weight_count_expected,
                         stats['positive_weight_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(positive_weight_count_expected,
                                 stats['positive_weight_count']))

        mean_expected = 57.3270868825
        self.assertAlmostEqual(mean_expected,
                               stats['mean'],
                               places=10,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_expected, stats['mean']))

    def test_vertex_frame_count(self):
        """
        Happy path test for count for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        count = user_frame.count(lambda row: row.age == 50)
        count_expected = 5
        self.assertEqual(count_expected,
                         count,
                         'Expected: {0}, Actual: {1}'
                         .format(count_expected, count))

    def test_vertex_frame_cumulative_percent(self):
        """
        Happy path test for cumulative_age for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        cum_return = user_frame.cumulative_percent('age')
        self.assertIsNone(cum_return)   # Confirm return type is correct.

        # Sort so last entry has max value.
        cum_percent = sorted(user_frame.take(user_frame.row_count,
                                             columns='age_cumulative_percent'))
        # If the last entry is right, all of the others should be too.
        cum_percent_expected = 1.0

        self.assertEqual(cum_percent_expected,
                         cum_percent[len(cum_percent) - 1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(cum_percent_expected,
                                 cum_percent[len(cum_percent) - 1][0]))

    def test_vertex_frame_cumulative_sum(self):
        """
        Happy path test for cumulative_sum for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        cum_return = user_frame.cumulative_sum('age')
        self.assertIsNone(cum_return)   # Confirm return type is correct.
        user_frame.sort('age')          # Sort so last entry has max value.
        cum_sum = user_frame.take(user_frame.row_count,
                                  columns='age_cumulative_sum')
        cum_sum = sorted(cum_sum)

        # If the last entry is right, all of the others should be too.
        # This is not the sum of the age column in the spreadsheet because
        # ten of those users rated multiple movies and their ages are not
        # duplicated in the graph (must be subtracted off the sum in
        # spreadsheet).
        cum_sum_expected = 33651.0
        self.assertEqual(cum_sum_expected,
                         cum_sum[len(cum_sum) - 1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(cum_sum_expected,
                                 cum_sum[len(cum_sum) - 1][0]))

    def test_vertex_frame_ecdf(self):
        """
        Happy path test for ecdf for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Check ECDF.  Vertex IDs are unique, so each cumulative figure
        # is 1/row_count higher.  There is no particular reason to take
        # 3 over any other integer: it allows for a smaller 'take'.
        ecdf_frame = user_frame.ecdf('_vid')
        ecdf_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        ecdf_take = ecdf_frame.take(20)
        check_row = 3
        self.assertEqual(ecdf_take[check_row - 1][1],
                         float(check_row) / user_frame.row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(ecdf_take[check_row - 1][1],
                                 float(check_row) / user_frame.row_count))

    def test_vertex_frame_entropy(self):
        """
        Happy path test for entropy for vertex frames.
        """

        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        def vcol_func(row):
            predict_wt = 1.5
            forecast = int(round(
                (row.percent // 18 + predict_wt * row.predict) /
                (1 + predict_wt)))

            influence = row.weight + 1
            return forecast, influence

        # Add 2 columns at once
        old_size = len(user_frame.schema)
        user_frame.add_columns(vcol_func, schema=[('forecast', ia.int32),
                                                  ('influence', ia.int32)])

        self.assertEqual(old_size + 2, len(user_frame.schema))
        self.assertIn('forecast', user_frame.column_names)
        self.assertIn('influence', user_frame.column_names)

        # Test Shannon entropy
        entropy = user_frame.entropy('percent', 'weight')
        entropy_expected = 4.279701826692489
        self.assertAlmostEqual(entropy,
                               entropy_expected,
                               places=14,
                               msg='Expected: {0}, Actual: {1}'
                               .format(entropy_expected, entropy))

    def test_vertex_frame_group_by(self):
        """
        Happy path test for group_by for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Check groupings; validate with weight of mode for group.
        agg_frame = user_frame.group_by('age', ia.agg.count)
        agg_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        agg_frame.sort([('count', False), ('age', True)])

        counts_expected = [[1, 1], [8, 1], [10, 1], [12, 5], [13, 2],
                           [14, 3], [15, 1], [16, 1], [17, 1], [18, 2],
                           [19, 2], [20, 3], [21, 1], [22, 2], [23, 3],
                           [24, 2], [25, 2], [26, 1], [28, 6], [29, 3],
                           [30, 15], [31, 11], [32, 11], [33, 8], [34, 9],
                           [35, 8], [36, 13], [37, 10], [38, 4], [39, 7],
                           [40, 6], [41, 4], [42, 6], [43, 9], [44, 7],
                           [45, 12], [46, 6], [47, 9], [48, 8], [49, 10],
                           [50, 5], [51, 6], [52, 7], [53, 11], [54, 6],
                           [55, 9], [56, 9], [57, 6], [58, 12], [59, 10],
                           [60, 10], [61, 9], [62, 9], [63, 8], [64, 13],
                           [65, 9], [66, 13], [67, 12], [68, 13], [69, 10],
                           [70, 10], [71, 11], [72, 14], [73, 9], [74, 10],
                           [75, 8], [76, 7], [77, 10], [78, 8], [79, 13],
                           [80, 11], [81, 7], [82, 7], [83, 5], [84, 4],
                           [85, 7], [86, 4], [87, 2], [88, 4], [89, 4],
                           [90, 2], [91, 4], [92, 3], [94, 5], [95, 3],
                           [96, 6], [97, 3], [99, 3]]

        counts = user_frame.group_by('age', ia.agg.count)

        row_count_expected = 88
        self.assertEqual(row_count_expected,
                         agg_frame.row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(row_count_expected, agg_frame.row_count))
        self.assertEqual(counts_expected,
                         sorted(counts.take(counts.row_count)),
                         'Expected: {0}, Actual: {1}'
                         .format(counts_expected,
                                 sorted(counts.take(counts.row_count))))

        agg_take = agg_frame.take(agg_frame.row_count)
        z = user_frame.column_mode('age')
        agg_count_list = [x[1] for x in agg_take]
        self.assertIn(z['weight_of_mode'], agg_count_list)    # mode weight

        # Check that original frame's length hasn't changed (TRIB-4233).
        self.assertEqual(self.user_node_count,
                         user_frame.row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.user_node_count, user_frame.row_count))

    def test_vertex_frame_histogram(self):
        """
        Happy path test for histogram for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        histogram = user_frame.histogram('age',
                                         num_bins=4,
                                         bin_type='equalwidth')

        cutoffs_expected = [1.0, 25.5, 50.0, 74.5, 99.0]
        self.assertEqual(cutoffs_expected,
                         histogram.cutoffs,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoffs_expected, histogram.cutoffs))

        hist_expected = [33.0, 183.0, 241.0, 130.0]
        self.assertEqual(hist_expected,
                         histogram.hist,
                         'Expected: {0}, Actual: {1}'
                         .format(hist_expected, histogram.hist))

        density_expected = [0.056218057921635436, 0.31175468483816016,
                            0.41056218057921634, 0.22146507666098808]
        self.assertEqual(density_expected,
                         histogram.density,
                         'Expected: {0}, Actual: {1}'
                         .format(density_expected, histogram.density))

        histogram2 = user_frame.histogram('age',
                                          num_bins=4,
                                          bin_type='equaldepth')

        cutoffs_expected = [1.0, 41.0, 60.0, 73.0, 99.0]
        self.assertEqual(cutoffs_expected,
                         histogram2.cutoffs,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoffs_expected, histogram2.cutoffs))

        hist_expected = [145.0, 152.0, 141.0, 149.0]
        self.assertEqual(hist_expected,
                         histogram2.hist,
                         'Expected: {0}, Actual: {1}'
                         .format(hist_expected, histogram2.hist))

        density_expected = [0.24701873935264054, 0.25894378194207834,
                            0.2402044293015332, 0.25383304940374785]
        self.assertEqual(density_expected,
                         histogram2.density,
                         'Expected: {0}, Actual: {1}'
                         .format(density_expected, histogram2.density))

    def test_vertex_frame_quantiles(self):
        """
        Happy path test for quantiles for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Compare 50th quantile against the median.
        quantile_frame = user_frame.quantiles('age', [25, 50, 75, 100])
        quantile_take = quantile_frame.take(10)
        median = float(user_frame.column_median('age'))
        self.assertEqual(quantile_take[1][1],
                         median,
                         'Expected: {0}, Actual: {1}'
                         .format(quantile_take[1][1], median))

    def test_vertex_frame_tally(self):
        """
        Happy path test for tally for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        user_frame.sort("_vid")
        tally_return = user_frame.tally('age', '50')
        self.assertIsNone(tally_return)
        user_frame.sort("_vid")
        tally = user_frame.take(user_frame.row_count,
                                columns='age_tally')

        tally_expected = 5
        self.assertEqual(tally_expected,
                         tally[-1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(tally_expected, tally[-1][0]))

    def test_vertex_frame_tally_percent(self):
        """
        Happy path test for tally_percent for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        user_frame.sort("_vid")
        tally_return = user_frame.tally_percent('age', '50')
        self.assertIsNone(tally_return)
        user_frame.sort("_vid")
        tally_percent = user_frame.take(user_frame.row_count,
                                        columns='age_tally_percent')

        # Middle element verified by inspection
        # Final element should be 100%
        tally_percent_expected = 1.0 / 5
        tally_total_expected = 1.00
        self.assertEqual(tally_total_expected,
                         tally_percent[-1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(tally_percent_expected,
                                 tally_percent[-1][0]))
        self.assertAlmostEqual(tally_percent_expected,
                               tally_percent[len(tally_percent)//2][0],
                               'Expected: {0}, Actual: {1}'
                               .format(tally_percent_expected,
                                       tally_percent[
                                           len(tally_percent)//2][0]))

    def test_vertex_frame_top_k(self):
        """
        Happy path test for top_k for vertex frames.
        """
        user_frame = self.graph.vertices['user']
        user_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        top_k = user_frame.top_k('age', 10)
        top_k_take = sorted(top_k.take(top_k.row_count))

        top_k_expected = [[30, 15.0], [36, 13.0], [45, 12.0], [58, 12.0],
                          [64, 13.0], [66, 13.0], [67, 12.0], [68, 13.0],
                          [72, 14.0], [79, 13.0]]
        self.assertEqual(top_k_expected,
                         top_k_take,
                         'Expected: {0}, Actual: {1}'
                         .format(top_k_expected, top_k_take))

    def test_export_csv_vertex(self):
        """ export and re-load a frame in CSV format """

        export_location = common_utils.get_a_name(self.data_csv_vertex)
        frame_orig = self.graph.vertices["movie"]
        print frame_orig.inspect(10)

        frame_orig.export_to_csv(export_location)

        # Re-load the exported CSV file.
        # Don't use build_frame: it makes assumptions about the file location.
        frame_csv = frame_utils.build_frame(
            export_location, self.schema_export_vertex, location=self.location, skip_header_lines=1)
        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort("_vid")
        take1 = frame_orig.take(20)
        frame_csv.sort("_vid")
        take2 = frame_csv.take(20)
        self.assertEqual(take1, take2)

    def test_export_json_vertex(self):
        """ export and re-load a frame in JSON format """

        export_location = common_utils.get_a_name(self.data_json_vertex)
        frame_orig = self.graph.vertices["movie"]
        print frame_orig.inspect(10)

        frame_orig.export_to_json(export_location)

        # Re-load the exported JSON file.
        # Don't use build_frame: it doesn't handle JSON sources.
        frame_json = frame_utils.build_frame(
            export_location, file_format="json", location=self.location)
        print "ORIGINAL\n", frame_orig.inspect(10)
        print "JSON COPY\n", frame_json.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_json.row_count)

        # TODO: convert to "mutate" flow when available
        def extract_json(row):
            """
            Return the listed fields from the file.
            Return 'None' for any missing element.
            """
            my_json = json.loads(row[0])
            _vid = my_json['_vid']
            _label = my_json['_label']
            movie = my_json['movie']
            return _vid, _label, movie

        frame_json.add_columns(extract_json, [("_vid", ia.int64),
                                              ("_label", str),
                                              ("movie", ia.int32)])
        frame_json.drop_columns("data_lines")
        print "JSON COPY PARSED\n", frame_json.inspect(10)

        # Check the file for data integrity
        frame_orig.sort("_vid")
        take1 = frame_orig.take(20)
        frame_json.sort("_vid")
        take2 = frame_json.take(20)
        self.assertEqual(take1, take2)


if __name__ == "__main__":
    unittest.main()
