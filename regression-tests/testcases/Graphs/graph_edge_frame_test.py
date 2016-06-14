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
   Usage:  python2.7 graph_edge_frame_test.py
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


class GraphEdgeFrameTest(atk_test.ATKTestCase):

    def setUp(self):
        # Call parent set-up.
        super(GraphEdgeFrameTest, self).setUp()
        self.location = cfg.export_location

        datafile = "movie_small.csv"
        self.data_csv_edge = "ex_csv_e"
        self.data_json_edge = "ex_json_e"
        self.schema_export_edge = [("_eid", ia.int64),
                                   ("_src_vid", ia.int64),
                                   ("_dest_vid", ia.int64),
                                   ("_label", str),
                                   ("rating", ia.int32)]

        # create basic movie graph
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

        # Create big frame
        self.frame = frame_utils.build_frame(
            datafile, self.schema, self.prefix, skip_header_lines=1)

        # Create graph
        self.graph = ia.Graph()
        self.graph.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Define vertices and edges
        self.graph.define_vertex_type('movie')
        self.graph.define_vertex_type('user')
        self.graph.define_edge_type('rating', 'user', 'movie', directed=True)

        # Add user vertices
        self.graph.vertices['user'].add_vertices(self.frame, 'user', ['age'])

        # Add movie vertices
        self.graph.vertices['movie'].add_vertices(self.frame, 'movie', [])

        #  Add edges
        self.graph.edges['rating'].add_edges(self.frame,
                                             'user',
                                             'movie',
                                             ['percent',
                                              'predict',
                                              'rating',
                                              'weight'],
                                             create_missing_vertices=False)

    # Check graph construction
        self.entry_count = 597  # Rows in data file
        self.user_node_count = 587
        self.movie_node_count = 10
        self.node_count = self.user_node_count + self.movie_node_count
        self.edge_count = 597

        # self.assertEqual(self.graph.vertices['user'].row_count,
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

    def test_edge_frame_assign_sample(self):
        """
        Confirm assign_sample passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Test exposure only; no validation of results.
        old_size = len(rating_frame.schema)
        rating_frame.assign_sample([0.3, 0.3, 0.4],
                                   ['train', 'test', 'validate'])
        self.assertEqual(old_size+1, len(rating_frame.schema),
                         "Expected: {0}, Actual: {1}"
                         .format(old_size+1, len(rating_frame.schema)))
        self.assertIn("sample_bin", rating_frame.column_names)

    def test_edge_frame_bin_column(self):
        """
        Confirm bin_column passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        bin_return = rating_frame.bin_column('rating',
                                             [0.0, 1.0, 2.0, 3.0, 4.0, 5.0])

        # Confirm there is no longer a return type issue.
        self.assertIsNone(bin_return)

        rating_frame.sort('rating')
        bin_take = rating_frame.take(rating_frame.row_count,
                                     columns=['rating', 'rating_binned'])

        # Check a couple of the bin boundaries.
        self.assertEqual(0,
                         bin_take[9][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[9][1]))
        self.assertEqual(1,
                         bin_take[10][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[10][1]))
        self.assertEqual(3,
                         bin_take[-200][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-200][1]))
        self.assertEqual(4,
                         bin_take[-199][1],
                         'Expected: {0}, Actual: {1}'
                         .format(4, bin_take[-199][1]))

    def test_edge_frame_bin_column_equal_depth(self):
        """
        Confirm bin_column_equal_depth passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        rating_frame.sort('rating')

        cutoff_points = rating_frame.bin_column_equal_depth('rating', 5)
        cutoff_expect = [-1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        self.assertEqual(cutoff_expect,
                         cutoff_points,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoff_expect, cutoff_points))

        rating_frame.sort('rating')
        bin_take = rating_frame.take(rating_frame.row_count,
                                     columns=['rating', 'rating_binned'])

        # Check a couple of the bin boundaries.
        self.assertEqual(0,
                         bin_take[42][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[42][1]))
        self.assertEqual(1,
                         bin_take[43][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[43][1]))
        self.assertEqual(3,
                         bin_take[-57][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-57][1]))
        self.assertEqual(4,
                         bin_take[-56][1],
                         'Expected: {0}, Actual: {1}'
                         .format(4, bin_take[-56][1]))

    def test_edge_frame_bin_column_equal_width(self):
        """
        Confirm bin_column_equal_width passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # bin_return is the list of cutoff points
        cutoff_points = rating_frame.bin_column_equal_width('rating', 5)
        print cutoff_points
        cutoff_expect = [-1.0, 0.4, 1.8, 3.2, 4.6, 6.0]
        for i, j in zip(cutoff_points, cutoff_expect):
            print i, j
            self.assertAlmostEqual(
                i, j,
                msg='Expected: {0}, Actual: {1}'.format(str(i), str(j)))

        rating_frame.sort('rating')
        bin_take = rating_frame.take(rating_frame.row_count,
                                     columns=['rating', 'rating_binned'])

        # Check a couple of the bin boundaries.
        self.assertEqual(0,
                         bin_take[9][1],
                         'Expected: {0}, Actual: {1}'
                         .format(0, bin_take[9][1]))
        self.assertEqual(1,
                         bin_take[10][1],
                         'Expected: {0}, Actual: {1}'
                         .format(1, bin_take[10][1]))
        self.assertEqual(3,
                         bin_take[-57][1],
                         'Expected: {0}, Actual: {1}'
                         .format(3, bin_take[-57][1]))
        self.assertEqual(4,
                         bin_take[-56][1],
                         'Expected: {0}, Actual: {1}'
                         .format(4, bin_take[-56][1]))

    def test_edge_frame_classification_metrics(self):
        """
        Confirm classification_metrics passes happy path test for edge frames.
        """

        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Add 2 columns, both to test that method and to provide data for
        # more complex operations
        def vcol_func(row):
            predict_wt = 1.5
            forecast = int(round(
                (row.percent // 18 + predict_wt * row.predict) /
                (1 + predict_wt)))

            influence = row.weight + 1
            return forecast, influence

        # Add 2 columns at once
        old_size = len(rating_frame.schema)
        rating_frame.add_columns(vcol_func, schema=[('forecast', ia.int32),
                                                    ('influence', ia.int32)])
        self.assertEqual(old_size + 2, len(rating_frame.schema))
        self.assertIn('forecast', rating_frame.column_names)
        self.assertIn('influence', rating_frame.column_names)

        # Test classification_metrics.  Results are a little off center
        # because there are so few positive entries
        cm = rating_frame.classification_metrics('predict', 'forecast')

        accuracy_expected = 0.542713567839196
        self.assertEqual(accuracy_expected,
                         cm.accuracy,
                         'Expected: {0}, Actual: {1}'
                         .format(accuracy_expected, cm.accuracy))

        recall_expected = accuracy_expected
        self.assertEqual(recall_expected,
                         cm.recall,
                         'Expected: {0}, Actual: {1}'
                         .format(recall_expected, cm.recall))

        f_measure_expected = 0.5365487298639426
        self.assertEqual(f_measure_expected,
                         cm.f_measure,
                         'Expected: {0}, Actual: {1}'
                         .format(f_measure_expected, cm.f_measure))

        precision_expected = 0.6638900484543208
        self.assertAlmostEqual(precision_expected,
                               cm.precision,
                               places=13,
                               msg='Expected: {0}, Actual: {1}'
                               .format(precision_expected, cm.precision))

        # confusion matrix should be not available for this table

    def test_edge_frame_column_median(self):
        """
        Confirm column_median passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        median = rating_frame.column_median('rating')
        median_expected = 3
        self.assertEqual(median_expected,
                         median,
                         'Expected: {0}, Actual: {1}'
                         .format(median_expected, median))

    def test_edge_frame_column_mode(self):
        """
        Confirm column_mode passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        mode = rating_frame.column_mode('rating')
        weight_of_mode_expected = 199
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

        modes_expected = [3]
        self.assertEqual(modes_expected,
                         mode['modes'],
                         'Expected: {0}, Actual: {1}'
                         .format(modes_expected, mode['modes']))

        total_weight_expected = self.edge_count
        self.assertEqual(total_weight_expected,
                         mode['total_weight'],
                         'Expected: {0}, Actual: {1}'
                         .format(total_weight_expected, mode['total_weight']))

    def test_edge_frame_column_summary_stats(self):
        """
        Confirm column_summary_statistics passes happy path test for edge
        frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        stats = rating_frame.column_summary_statistics('rating')

        row_count_expected = self.edge_count
        self.assertEqual(row_count_expected,
                         stats['good_row_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(row_count_expected, stats['good_row_count']))

        non_pos_weight_count_expected = 0
        self.assertEqual(non_pos_weight_count_expected,
                         stats['non_positive_weight_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(non_pos_weight_count_expected,
                                 stats['non_positive_weight_count']))

        std_deviation_expected = 1.14538232335
        self.assertAlmostEqual(std_deviation_expected,
                               stats['standard_deviation'],
                               places=11,
                               msg='Expected: {0}, Actual: {1}'
                               .format(std_deviation_expected,
                                       stats['standard_deviation']))

        max_expected = 6.0
        self.assertEqual(max_expected,
                         stats['maximum'],
                         'Expected: {0}, Actual: {1}'
                         .format(max_expected, stats['maximum']))

        mean_confidence_upper_expected = 3.10527999559
        self.assertAlmostEqual(mean_confidence_upper_expected,
                               stats['mean_confidence_upper'],
                               places=11,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_confidence_upper_expected,
                                       stats['mean_confidence_upper']))

        mean_confidence_lower_expected = 2.92152067443
        self.assertAlmostEqual(mean_confidence_lower_expected,
                               stats['mean_confidence_lower'],
                               places=11,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_confidence_lower_expected,
                                       stats['mean_confidence_lower']))

        min_expected = -1.0
        self.assertEqual(min_expected,
                         stats['minimum'],
                         'Expected: {0}, Actual: {1}'
                         .format(min_expected, stats['minimum']))

        total_weight_expected = self.edge_count
        self.assertEqual(total_weight_expected,
                         stats['total_weight'],
                         'Expected: {0}, Actual: {1}'
                         .format(total_weight_expected, stats['total_weight']))

        bad_row_count_expected = 0
        self.assertEqual(bad_row_count_expected,
                         stats['bad_row_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(bad_row_count_expected,
                                 stats['bad_row_count']))

        variance_expected = 1.3119006666441828
        self.assertAlmostEqual(variance_expected,
                               stats['variance'],
                               places=9,
                               msg='Expected: {0}, Actual: {1}'
                               .format(variance_expected, stats['variance']))

        geometric_mean_expected = None
        self.assertEqual(geometric_mean_expected,
                         stats['geometric_mean'],
                         'Expected: {0}, Actual: {1}'
                         .format(geometric_mean_expected,
                                 stats['geometric_mean']))

        positive_weight_count_expected = self.edge_count
        self.assertEqual(positive_weight_count_expected,
                         stats['positive_weight_count'],
                         'Expected: {0}, Actual: {1}'
                         .format(positive_weight_count_expected,
                                 stats['positive_weight_count']))

        mean_expected = 3.013400335008375
        self.assertAlmostEqual(mean_expected,
                               stats['mean'],
                               places=15,
                               msg='Expected: {0}, Actual: {1}'
                               .format(mean_expected, stats['mean']))

    def test_edge_frame_count(self):
        """
        Confirm count passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup
        count = rating_frame.count(lambda row: row.rating == 3)
        count_expected = 199
        self.assertEqual(count_expected,
                         count,
                         'Expected: {0}, Actual: {1}'
                         .format(count_expected, count))

    def test_edge_frame_cumulative_percent(self):
        """
        Confirm cumulative_percent passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        cum_return = rating_frame.cumulative_percent('rating')  # Not a frame.
        self.assertIsNone(cum_return)   # Confirm return type is correct.

        # Sort so last entry has max value.
        rating_frame.sort('rating_cumulative_percent')
        cum_percent = rating_frame.take(rating_frame.row_count,
                                        columns='rating_cumulative_percent')

        # If the last entry is right, all of the others should be too.
        cum_percent_expected = 1.0
        self.assertEqual(cum_percent_expected,
                         cum_percent[len(cum_percent) - 1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(cum_percent_expected,
                                 cum_percent[len(cum_percent) - 1][0]))

    def test_edge_frame_cumulative_sum(self):
        """
        Confirm cumulative_sum passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        cum_return = rating_frame.cumulative_sum('rating')  # Not a frame; None
        self.assertIsNone(cum_return)   # Confirm return type is correct.

        rating_frame.sort('rating')     # Sort so last entry has max value.
        cum_sum = rating_frame.take(rating_frame.row_count,
                                    columns='rating_cumulative_sum')
        cum_sum = sorted(cum_sum)

        # If the last entry is right, all of the others should be too.
        # This is not the sum of the age column in the spreadsheet because
        # ten of those users rated multiple movies and their ages are not
        # duplicated in the graph (must be subtracted off the sum in
        # spreadsheet).
        cum_sum_expected = 1799.0
        self.assertEqual(cum_sum_expected,
                         cum_sum[len(cum_sum) - 1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(cum_sum_expected,
                                 cum_sum[len(cum_sum) - 1][0]))

    def test_edge_frame_ecdf(self):
        """
        Confirm ecdf passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Check ECDF.  Edge IDs are unique, so each cumulative figure
        # is 1/row_count higher.  There is no particular reason to take
        # 3 over any other integer: it allows for a smaller 'take'.
        ecdf_frame = rating_frame.ecdf('_eid')
        ecdf_take = ecdf_frame.take(20)
        check_row = 3
        self.assertEqual(float(check_row) / rating_frame.row_count,
                         ecdf_take[check_row - 1][1],
                         'Expected: {0}, Actual: {1}'
                         .format(float(check_row) / rating_frame.row_count,
                                 ecdf_take[check_row - 1][1]))

    def test_edge_frame_entropy(self):
        """
        Confirm entropy passes happy path test for edge frames.
        """

        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Add 2 columns, both to test that method and to provide data for
        # more complex operations
        def vcol_func(row):
            predict_wt = 1.5
            forecast = int(round(
                (row.percent // 18 + predict_wt * row.predict) /
                (1 + predict_wt)))

            influence = row.weight + 1
            return forecast, influence

        # Add 2 columns at once
        old_size = len(rating_frame.schema)
        rating_frame.add_columns(vcol_func, schema=[('forecast', ia.int32),
                                                    ('influence', ia.int32)])
        self.assertEqual(old_size + 2, len(rating_frame.schema))
        self.assertIn('forecast', rating_frame.column_names)
        self.assertIn('influence', rating_frame.column_names)

        # Test Shannon entropy.
        entropy = rating_frame.entropy('percent', 'weight')
        print "entropy = {0}".format(entropy)
        # self.assertAlmostEqual(entropy, 4.28, places=1)
        entropy_expected = 4.28111204154
        self.assertAlmostEqual(entropy,
                               entropy_expected,
                               places=11,
                               msg='Expected: {0}, Actual: {1}'
                               .format(entropy_expected, entropy))

    def test_edge_frame_group_by(self):
        """
        Confirm group_by passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Check groupings; validate with weight of mode for group
        agg_frame = rating_frame.group_by('rating', ia.agg.count)
        agg_frame.name = common_utils.get_a_name(self.prefix)     # for cleanup
        agg_frame.sort([('count', False), ('rating', True)])
        counts = rating_frame.group_by('rating', ia.agg.count)

        row_count_expected = 8
        self.assertEqual(row_count_expected,
                         agg_frame.row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(row_count_expected, agg_frame.row_count))

        counts_expected = [[-1, 1], [0, 9], [1, 33], [2, 156], [3, 199],
                           [4, 143], [5, 50], [6, 6]]
        self.assertEqual(counts_expected,
                         sorted(counts.take(counts.row_count)),
                         'Expected: {0}, Actual: {1}'
                         .format(counts_expected,
                                 sorted(counts.take(counts.row_count))))

        agg_take = agg_frame.take(agg_frame.row_count)
        z = rating_frame.column_mode('rating')
        agg_count_list = [x[1] for x in agg_take]
        self.assertIn(z['weight_of_mode'], agg_count_list)    # mode weight

        # Check that frame length hasn't changed after group_by (TRIB-4233).
        self.assertEqual(self.edge_count,
                         rating_frame.row_count,
                         'Expected: {0}, Actual: {1}'
                         .format(self.edge_count, rating_frame.row_count))

    def test_edge_frame_histogram(self):
        """
        Confirm histogram passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Equal width
        histogram = rating_frame.histogram('rating',
                                           num_bins=4,
                                           bin_type='equalwidth')

        cutoffs_expected = [-1.0, 0.75, 2.5, 4.25, 6.0]
        self.assertEqual(cutoffs_expected,
                         histogram.cutoffs,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoffs_expected, histogram.cutoffs))

        hist_expected = [10.0, 189.0, 342.0, 56.0]
        self.assertEqual(hist_expected,
                         histogram.hist,
                         'Expected: {0}, Actual: {1}'
                         .format(hist_expected, histogram.hist))

        density_expected = [0.01675041876046901, 0.3165829145728643,
                            0.5728643216080402, 0.09380234505862646]
        self.assertEqual(density_expected,
                         histogram.density,
                         'Expected: {0}, Actual: {1}'
                         .format(density_expected, histogram.density))

        # Equal depth
        histogram2 = rating_frame.histogram('rating',
                                            num_bins=4,
                                            bin_type='equaldepth')

        cutoffs_expected = [-1.0, 3.0, 4.0, 6.0]
        self.assertEqual(cutoffs_expected,
                         histogram2.cutoffs,
                         'Expected: {0}, Actual: {1}'
                         .format(cutoffs_expected, histogram2.cutoffs))

        hist_expected = [199.0, 199.0, 199.0]
        self.assertEqual(hist_expected,
                         histogram2.hist,
                         'Expected: {0}, Actual: {1}'
                         .format(hist_expected, histogram2.hist))

        density_expected = [0.3333333333333333, 0.3333333333333333,
                            0.3333333333333333]
        self.assertEqual(density_expected,
                         histogram2.density,
                         'Expected: {0}, Actual: {1}'
                         .format(density_expected, histogram2.density))

    def test_edge_frame_quantiles(self):
        """
        Confirm quantiles passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        # Compare 50th quantile against the median.
        quantile_frame = rating_frame.quantiles('rating', [25, 50, 75, 100])
        quantile_take = quantile_frame.take(10)
        median = float(rating_frame.column_median('rating'))
        self.assertEqual(quantile_take[1][1],
                         median,
                         'Expected: {0}, Actual: {1}'
                         .format(quantile_take[1][1], median))

    def test_edge_frame_tally(self):
        """
        Confirm tally passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        rating_frame.sort("_eid")
        tally_return = rating_frame.tally('rating', '5')  # Not a frame; None
        self.assertIsNone(tally_return)
        rating_frame.sort("_eid")
        tally = rating_frame.take(rating_frame.row_count,
                                  columns='rating_tally')

        tally_expected = 50
        self.assertEqual(tally_expected,
                         tally[-1][0],
                         'Expected: {0}, Actual: {1}'
                         .format(tally_expected, tally[-1][0], ))

    def test_edge_frame_tally_percent(self):
        """
        Confirm tally_percent passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        rating_frame.sort("_eid")
        rating_frame.tally('rating', '4')  # None
        tally_return = rating_frame.tally_percent('rating', '4')  # None
        self.assertIsNone(tally_return)
        rating_frame.sort("_eid")
        tally_percent = rating_frame.take(rating_frame.row_count,
                                          columns='rating_tally_percent')

        # Middle element verified by inspection
        # Final element should be 100%
        tally_percent_expected = 67.0/143
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

    def test_edge_frame_top_k(self):
        """
        Confirm top_k passes happy path test for edge frames.
        """
        rating_frame = self.graph.edges['rating']
        rating_frame.name = common_utils.get_a_name(self.prefix)  # for cleanup

        top_k = rating_frame.top_k('rating', 4)
        top_k_take = sorted(top_k.take(top_k.row_count))

        top_k_expected = [[2, 156.0], [3, 199.0], [4, 143.0], [5, 50.0]]
        self.assertEqual(top_k_expected,
                         top_k_take,
                         'Expected: {0}, Actual: {1}'
                         .format(top_k_expected, top_k_take))

    def test_export_csv_edge(self):
        """ export and re-load a frame in CSV format """

        name = common_utils.get_a_name(self.data_csv_edge)
        frame_orig = self.graph.edges["rating"]
        frame_orig.drop_columns(["percent", "predict", "weight"])

        frame_orig.export_to_csv(name)

        # Re-load the exported CSV file.
        frame_csv = frame_utils.build_frame(name,
                                            self.schema_export_edge,
                                            self.prefix,
                                            location=self.location,
                                            skip_header_lines=1)
        print "ORIGINAL\n", frame_orig.inspect(10)
        print "CSV COPY\n", frame_csv.inspect(10)
        self.assertEqual(frame_orig.row_count, frame_csv.row_count)

        # Check the file for data integrity
        frame_orig.sort("_eid")
        take1 = frame_orig.take(20)
        frame_csv.sort("_eid")
        take2 = frame_csv.take(20)
        self.assertEqual(take1, take2)

    def test_export_json_edge(self):
        """ export and re-load a frame in JSON format """

        export_location = common_utils.get_a_name(self.data_json_edge)
        frame_orig = self.graph.edges["rating"]
        frame_orig.drop_columns(["percent", "predict", "weight"])

        frame_orig.export_to_json(export_location)

        # Re-load the exported JSON file.
        frame_json = frame_utils.build_frame(
            export_location, prefix=self.prefix,
            file_format="json", location=self.location)
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
            _vid = my_json['_eid']
            _label = my_json['_label']
            movie = my_json['rating']
            return _vid, _label, movie

        frame_json.add_columns(extract_json, [("_eid", ia.int64),
                                              ("_label", str),
                                              ("rating", ia.int32)])
        frame_json.drop_columns("data_lines")
        print "JSON COPY PARSED\n", frame_json.inspect(10)

        # Check the file for data integrity
        frame_orig.sort("_eid")
        take1 = frame_orig.take(20, columns=["_eid", "_label", "rating"])
        frame_json.sort("_eid")
        take2 = frame_json.take(20, columns=["_eid", "_label", "rating"])
        self.assertEqual(take1, take2)


if __name__ == "__main__":
    unittest.main()
