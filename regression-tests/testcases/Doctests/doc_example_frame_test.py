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
    Usage:  python2.7 doc_example_frame_test.py
    Test column accumulation methods.
"""
__author__ = 'Prune Wickart'
__credits__ = ["Prune Wickart"]
__version__ = "2015.06.05"

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import unittest

import trustedanalytics as ia
from qalib import frame_utils
from qalib import atk_test


class TestDocExamples(atk_test.ATKTestCase):

    def setUp(self):
        super(TestDocExamples, self).setUp()

        self.example_data = "example_count_v2.csv"
        self.example_schema = [("idnum", ia.int32)]

        self.bin_col_data = "bin_col_example.csv"
        self.bin_col_schema = [("a", ia.int32)]

        self.example_classification_data = "classification_example.csv"
        self.example_classification_schema = [("a", str),
                                              ("b", ia.int32),
                                              ("labels", ia.int32),
                                              ("predictions", ia.int32)]

        self.example_covar_cols_data = "covar_example_cols.csv"
        self.example_covar_cols_schema = [("col_0", ia.int64),
                                          ("col_1", ia.int64),
                                          ("col_2", ia.float32)]

        self.example_covar_vector_data = "covar_example_vector.csv"
        self.example_covar_vector_schema = [("State", str),
                                            ("index", ia.int32)]

        self.dot_product_schema = [("col_0", ia.int32)]
        self.flatten_schema = [("a", ia.int32)]

    def test_bin_column_example(self):
        """
        bin_column examples;
            accumulate the binnings for convenience.
        """
        expected = [[1, -1, 0, 0, 0],
                    [1, -1, 0, 0, 0],
                    [2, -1, 0, 0, 0],
                    [3, -1, 0, 0, 0],
                    [5, 0, 0, 1, 0],
                    [8, 0, 0, 1, 1],
                    [13, 1, 1, 1, 1],
                    [21, 1, 1, 1, 1],
                    [34, 2, 2, 2, 1],
                    [55, 2, 2, 3, 2],
                    [89, -1, 2, 3, 3]]

        my_frame = frame_utils.build_frame(
            self.bin_col_data, self.bin_col_schema)

        my_frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
                            strict_binning=True, bin_column_name='bin1')
        my_frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
                            strict_binning=False, bin_column_name='bin2')
        my_frame.bin_column('a', [1, 5, 34, 55, 89], include_lowest=True,
                            strict_binning=False, bin_column_name='bin3')
        my_frame.bin_column('a', [1, 5, 34, 55, 89], include_lowest=False,
                            strict_binning=True, bin_column_name='bin4')

        # Check results against those listed in user doc.
        my_frame.sort('a')
        self.assertEqual(expected, my_frame.take(my_frame.row_count))

    def test_bin_column_eq_example(self):
        """
        Examples of bin_column_equal_depth/_width;
            accumulate the binnings for convenience.
        """
        my_frame = frame_utils.build_frame(
            self.bin_col_data, self.bin_col_schema)
        expected_ed = [1.0, 2.0, 5.0, 13.0, 34.0, 89.0]
        expected_ew = [1.0, 18.6, 36.2, 53.8, 71.4, 89.0]
        expected_take = [[1, 0, 0],
                         [1, 0, 0],
                         [2, 1, 0],
                         [3, 1, 0],
                         [5, 2, 0],
                         [8, 2, 0],
                         [13, 3, 0],
                         [21, 3, 1],
                         [34, 4, 1],
                         [55, 4, 3],
                         [89, 4, 4]]

        cutoffs_ed = my_frame.bin_column_equal_depth('a', 5, 'aEDBinned')
        cutoffs_ew = my_frame.bin_column_equal_width('a', 5, 'aEWBinned')
        my_frame.sort('a')
        cutoffs_ed = [round(x, 1) for x in cutoffs_ed]
        cutoffs_ew = [round(x, 1) for x in cutoffs_ew]
        frame_take = my_frame.take(my_frame.row_count)
        print frame_take
        print my_frame.inspect(my_frame.row_count)
        self.assertEqual(expected_ed, cutoffs_ed)
        self.assertEqual(expected_ew, cutoffs_ew)
        self.assertEqual(expected_take, frame_take)

    def test_classification_metrics_example(self):
        """
        classification_metrics example
        """
        # Note: This example is no longer in the documentation.

        frame = frame_utils.build_frame(self.example_classification_data,
                                        self.example_classification_schema,
                                        skip_header_lines=1)
        expected_f_measure = 2/3.0
        expected_recall = 1/2.0
        expected_accuracy = 3/4.0
        expected_precision = 1/1.0

        print frame.inspect()
        cm = frame.classification_metrics(label_column='labels',
                                          pred_column='predictions',
                                          pos_label=1,
                                          beta=1)
        print cm
        self.assertEqual(expected_f_measure, cm.f_measure)
        self.assertEqual(expected_recall, cm.recall)
        self.assertEqual(expected_accuracy, cm.accuracy)
        self.assertEqual(expected_precision, cm.precision)

    def test_correlation(self):
        """
        correlation and correlation_matrix examples
        """
        # Check the coefficients (Person's correlation) for both
        #   column-to-column and full cross-column correlations.
        # Round the results to allow for digital precision.

        import math

        precision = 6   # Digits of precision demanded

        c3 = 3 / math.sqrt(10)   # correlation with column x3
        c3_round = round(c3, precision)

        block_data = [
            [1, 4, 0, -1],
            [2, 3, 0, -1],
            [3, 2, 1, -1],
            [4, 1, 2, -1],
            [5, 0, 2, -1]
        ]

        expected_take = [
            [1, 1, -1, c3_round, 0],
            [1, 1, -1, c3_round, 0],
            [-1, -1, 1, -c3_round, 0],
            [c3_round, c3_round, -c3_round, 1, 0],
            [0, 0, 0, 0, 1]
        ]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum < len(block_data))
        my_frame.add_columns(complete_data,
                             [("x1", ia.float32),
                              ("x2", ia.float32),
                              ("x3", ia.float32),
                              ("x4", ia.float32)
                              ])

        print my_frame.inspect()
        corr12 = my_frame.correlation(["x1", "x2"])
        self.assertAlmostEqual(-1.00, corr12)
        corr14 = my_frame.correlation(["x1", "x4"])
        self.assertAlmostEqual(+0.00, corr14)
        corr23 = my_frame.correlation(["x2", "x3"])
        self.assertAlmostEqual(-c3, corr23)

        corr_matrix = my_frame.correlation_matrix(my_frame.column_names)
        print corr_matrix.inspect()
        corr_take = corr_matrix.take(corr_matrix.row_count)
        tally_take = [[round(x, precision)
                       for x in y] for y in corr_take]
        print tally_take
        self.assertEqual(expected_take, tally_take)

    def test_covar_example_cols(self):
        """
        covariance_matrix example
        """
        my_frame1 = frame_utils.build_frame(
            self.example_covar_cols_data, self.example_covar_cols_schema)
        expected_covar_take = [[1.00, 1.00, -6.65],
                               [1.00, 1.00, -6.65],
                               [-6.65, -6.65, 139.99]]

        cov_matrix = my_frame1.covariance_matrix(['col_0', 'col_1', 'col_2'])
        cov_take = cov_matrix.take(cov_matrix.row_count)
        cov_take_round = [[round(x, 2) for x in y] for y in cov_take]

        self.assertEqual(expected_covar_take, cov_take_round)

    def test_covar_example_vector(self):
        """
        covariance_matrix example, with vector input
        """
        def fetch_vector(row):
            vector_data = [[0.0, 1.0, 0.0, 0.0],
                           [0.0, 1.0, 0.0, 0.0],
                           [0.0, 0.54, 0.46, 0.0],
                           [0.0, 0.83, 0.17, 0.0]]
            return vector_data[row.index]

        my_frame2 = frame_utils.build_frame(
            self.example_covar_vector_data, self.example_covar_vector_schema)

        expected_covar_take = [[0,  0.00000000,  0.00000000,    0],
                               [0,  0.04709167, -0.04709167,    0],
                               [0, -0.04709167,  0.04709167,    0],
                               [0,  0.00000000,  0.00000000,    0]]

        my_frame2.add_columns(fetch_vector,
                              ('Population_HISTOGRAM', ia.vector(4)))
        my_frame2.drop_columns("index")
        print my_frame2.inspect()
        covar_matrix = my_frame2.covariance_matrix(['Population_HISTOGRAM'])
        covar_take = [[round(x, 8) for x in y[0]] for y in
                      covar_matrix.take(covar_matrix.row_count)]
        print covar_take
        # cov_take_round = [[round(x,2) for x in y] for y in cov_take]

        self.assertEqual(expected_covar_take, covar_take)

    def test_cumu_example(self):
        """
        cumulative_sum and cumulative_percent
        """
        def fetch_frame(row):
            return row.idnum % 3

        expected_cumu_take = [[0, 0, 0, round(0/6.0, 3)],
                              [1, 1, 1, round(1/6.0, 3)],
                              [2, 2, 3, round(3/6.0, 3)],
                              [3, 0, 3, round(3/6.0, 3)],
                              [4, 1, 4, round(4/6.0, 3)],
                              [5, 2, 6, round(6/6.0, 3)]]

        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.add_columns(fetch_frame, ("obs", ia.int32))
        my_frame.filter(lambda (row): row.idnum in range(6))
        my_frame.sort("idnum")
        print "INITIAL DATA\n", my_frame.inspect()

        my_frame.cumulative_sum('obs')
        print "AFTER SUM\n", my_frame.inspect()    # added
        my_frame.sort("idnum")
        print "BEFORE PCT\n", my_frame.inspect()    # added
        my_frame.cumulative_percent('obs')
        my_frame.sort("idnum")
        print "AFTER PCT & RE-SORT\n", my_frame.inspect()

        cumu_take = [x[:3] + [round(x[3], 3)]
                     for x in my_frame.take(my_frame.row_count)]
        print cumu_take

        self.assertEqual(expected_cumu_take, cumu_take)

    def test_drop_duplicates(self):
        """
        drop_duplicates examples in sequence
        """
        # Currently, this is the reverse of the sequence in the documentation,
        # but this may change per DPAT-249.
        frame_load = [[200, 4, 25],
                      [200, 5, 25],
                      [200, 4, 25],
                      [200, 5, 35],
                      [200, 6, 25],
                      [200, 8, 35],
                      [200, 4, 45],
                      [200, 4, 25],
                      [200, 5, 25],
                      [200, 5, 35],
                      [201, 4, 25]]

        # expected_prod_take = [[200, 4, 25],
        #                       [200, 5, 35]]

        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        print my_frame.row_count, "rows in data file;"
        print len(frame_load), "rows in working set"
        my_frame.filter(lambda (row): row.idnum in range(len(frame_load)))

        def complete_data(row):
            a, b, c = frame_load[row.idnum]
            return a, b, c

        my_frame.add_columns(complete_data, [("a", ia.int32),
                                             ("b", ia.int32),
                                             ("c", ia.int32)])
        my_frame.drop_columns("idnum")
        print my_frame.inspect(20)
        self.assertEqual(my_frame.row_count, 11)
        my_frame.drop_duplicates()
        print my_frame.inspect(20)
        self.assertEqual(my_frame.row_count, 7)
        my_frame.drop_duplicates(["a", "c"])
        print my_frame.inspect(20)
        self.assertEqual(my_frame.row_count, 4)
        my_frame.drop_duplicates("b")
        print my_frame.inspect(20)
        self.assertLessEqual(my_frame.row_count, 3)     # Either 2 or 3 rows

    def test_dot_product(self):
        """
        dot_product examples, both column and vector
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.dot_product_schema)
        my_frame.filter(lambda (row): row.col_0 in range(1, 6))

        expected_prod_take = [
            [1, 0.20, -2, 5, [1.0, 0.2], [-2.0, 5.0], -1, -1, -1.0],
            [2, 0.40, -1, 6, [2.0, 0.4], [-1.0, 6.0], 0.40, 0.40, 0.40],
            [3, 0.60, 0, 7, [3.0, 0.6], [0.0, 7.0], 4.2, 4.2, 4.2],
            [4, 0.80, 1, 8, [4.0, 0.8], [1.0, 8.0], 10.4, 10.4, 10.4],
            [5, None, 2, None, [5.0, None], [2.0, None], 10.0, 10.08, 10.0]]

        def complete_data(row):
            col_1 = None if row.col_0 == 5 else round(row.col_0 / 5.0, 2)
            col_2 = row.col_0 - 3
            col_3 = None if row.col_0 == 5 else row.col_0 + 4
            col_4 = [row.col_0, col_1]
            col_5 = [col_2, col_3]
            return col_1, col_2, col_3, col_4, col_5

        my_frame.add_columns(complete_data, [("col_1", ia.float32),
                                             ("col_2", ia.int32),
                                             ("col_3", ia.int32),
                                             ("col_4", ia.vector(2)),
                                             ("col_5", ia.vector(2))])
        print my_frame.inspect(my_frame.row_count)

        my_frame.dot_product(['col_0', 'col_1'], ['col_2', 'col_3'],
                             'dot_product')
        my_frame.dot_product(['col_0', 'col_1'], ['col_2', 'col_3'],
                             'dot_product_2', [0.1, 0.2], [0.3, 0.4])
        my_frame.dot_product('col_4', 'col_5', 'dot_product_v')

        # Round off all vector (list) components; leave other things alone.
        my_frame_take = [y if not isinstance(y, list) else
                         [round(x, 2) if isinstance(x, float) else x
                          for x in y]
                         for y in my_frame.take(my_frame.row_count)]
        print my_frame_take
        # Dot product doesnt guarantee result order
        self.assertEqual(expected_prod_take.sort(), my_frame_take.sort())

    def test_flatten(self):
        """
        flatten_column example
        unflatten_column example
        """
        block_data = ["",
                      "solo,mono,single",
                      "duo,double"]
        # frame after flatten
        expected_prod_take = [
            [1, "mono"],
            [1, "single"],
            [1, "solo"],
            [2, "double"],
            [2, "duo"]
        ]
        # frame after unflatten: note that strings are alpha-sorted.
        expected_unflatten_take = [
            [1, "mono,single,solo"],
            [2, "double,duo"]
        ]

        my_frame = frame_utils.build_frame(
            self.example_data, self.flatten_schema)
        my_frame.filter(lambda (row): row.a in range(1, len(block_data)))

        def complete_data(row):
            return block_data[row.a]

        my_frame.add_columns(complete_data, ('b', str))
        print my_frame.inspect(my_frame.row_count)

        my_frame.flatten_columns('b')
        my_frame.sort(['a', 'b'])
        my_frame_take = my_frame.take(my_frame.row_count)
        print my_frame_take
        print my_frame.inspect(my_frame.row_count)
        self.assertEqual(expected_prod_take, my_frame_take)

        my_frame.unflatten_columns(['a'])
        my_frame.sort('a')
        print my_frame.inspect(my_frame.row_count)
        my_frame_take = my_frame.take(my_frame.row_count)
        for row in range(len(expected_unflatten_take)):
            mound_expected = sorted(expected_unflatten_take[row][1].split(','))
            mound_actual = sorted(my_frame_take[row][1].split(','))
            # print row, mound_expected, mound_actual
            self.assertEqual(mound_expected, mound_actual)

    def test_group_by_agg(self):
        """
        group_by example with count
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(6))

        block_data = ["cat", "apple", "bat", "cat", "bat", "cat"]
        expected_prod_take = [
            ["apple", 1],
            ["bat", 2],
            ["cat", 3]
        ]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, ("a", str))
        print my_frame.inspect(my_frame.row_count)

        new_frame = my_frame.group_by('a', ia.agg.count)
        new_frame.sort('a')
        new_frame_take = new_frame.take(new_frame.row_count)
        print new_frame_take
        self.assertEqual(expected_prod_take, new_frame_take)

    def test_group_by_avg(self):
        """
        group_by example with average
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(5))

        block_data = [
            (1, "alpha", 3.0),
            (1, "bravo", 5.0),
            (1, "alpha", 5.0),
            (2, "bravo", 8.0),
            (2, "bravo", 12.0)]
        expected_prod_take = [
            [1, "alpha", 4.0],
            [1, "bravo", 5.0],
            [2, "bravo", 10.0]
        ]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [("a", ia.int32),
                                             ("b", str),
                                             ("c", ia.float64)])
        my_frame.drop_columns('idnum')
        print my_frame.inspect(my_frame.row_count)

        new_frame = my_frame.group_by(['a', 'b'], {'c': ia.agg.avg})
        new_frame.sort(['a', 'b'])
        new_frame_take = new_frame.take(new_frame.row_count)
        print new_frame_take
        self.assertEqual(expected_prod_take, new_frame_take)

    def test_group_by_stats(self):
        """
        group_by example with several attributes checked
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(5))

        block_data = [
            ("ape", 1, 4.0, 9),
            ("ape", 1, 8.0, 8),
            ("big", 1, 5.0, 7),
            ("big", 1, 6.0, 6),
            ("big", 1, 8.0, 5)]
        expected_take = {u'a': {0: u'ape', 1: u'big'},
                         u'count': {0: 2, 1: 3},
                         u'c': {0: 1, 1: 1},
                         u'd_SUM': {0: 12.0, 1: 19.0},
                         u'd_MIN': {0: 4.0, 1: 5.0},
                         u'd_AVG': {0: 6.0, 1: 6.333333333333333},
                         u'e_MAX': {0: 9, 1: 7}}

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [("a", str),
                                             ("c", ia.int32),
                                             ("d", ia.float64),
                                             ("e", ia.int32)])
        my_frame.drop_columns('idnum')
        print my_frame.inspect(my_frame.row_count)

        new_frame = my_frame.group_by(
            ['a', 'c'], ia.agg.count,
            {'d': [ia.agg.avg, ia.agg.sum, ia.agg.min], 'e': ia.agg.max})
        new_frame.sort('a')
        print new_frame.inspect(10)
        new_res = new_frame.download(new_frame.row_count)
        print new_res
        self.assertEqual(new_res.to_dict(), expected_take)

    def test_histogram(self):
        """
        Histogram example
        """
        frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        frame.filter(lambda (row): row.idnum in range(5))

        block_data = [
            ("a", 2),
            ("b", 7),
            ("c", 3),
            ("d", 9),
            ("e", 1)]

        expected_width_cutoffs = [1.0, 1 + 8.0/3, 1 + 2*8.0/3, 9.0]
        expected_width_hist = [3, 0, 2]
        expected_width_density = [0.6, 0.0, 0.4]

        expected_depth_cutoffs = [1, 2, 7, 9]
        expected_depth_hist = [1, 2, 2]
        expected_depth_density = [0.2, 0.4, 0.4]

        def complete_data(row):
            return block_data[row.idnum]

        frame.add_columns(complete_data, [("a", str),
                                          ("b", ia.int32)])
        frame.drop_columns('idnum')
        print frame.inspect(frame.row_count)

        hist = frame.histogram("b", num_bins=3)
        print hist
        self.assertEqual(expected_width_cutoffs, hist.cutoffs)
        self.assertEqual(expected_width_hist, hist.hist)
        self.assertEqual(expected_width_density, hist.density)

        hist = frame.histogram("b", num_bins=3, bin_type='equaldepth')
        print hist
        self.assertEqual(expected_depth_cutoffs, hist.cutoffs)
        self.assertEqual(expected_depth_hist, hist.hist)
        self.assertEqual(expected_depth_density, hist.density)

    def test_join_left(self):
        """
        Left join example
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(4))
        your_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        your_frame.filter(lambda (row): row.idnum in range(4, 7))

        block_data = [
            # my_frame, rows 0-3
            ("alligator", "bear", "cat"),
            ("auto", "bus", "car"),
            ("apple", "berry", "cantaloupe"),
            ("mirror", "frog", "ball"),
            # your_frame, rows 4-6
            ("bus", 871, "dog"),
            ("berry", 5218, "frog"),
            ("blue", 0, "log")]

        # Correct output
        expected_take = [[u'alligator', u'bear', u'cat', None, None],
                         [u'apple', u'berry', u'cantaloupe', 5218.0, u'frog'],
                         [u'auto', u'bus', u'car', 871.0, u'dog'],
                         [u'mirror', u'frog', u'ball', None, None]]
        # Output given under https://intel-data.atlassian.net/browse/DPAT-439
        # expected_take = [[u'alligator', u'bear', u'cat', None, None, None],
        #                  [u'apple', u'berry', u'cantaloupe', u'berry',
        #                   5218.0, u'frog'],
        #                  [u'auto', u'bus', u'car', u'bus', 871.0, u'dog'],
        #                  [u'mirror', u'frog', u'ball', None, None, None]]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [
            ("a", str),
            ("b", str),
            ("c", str)])
        my_frame.drop_columns('idnum')
        my_frame.sort('b')
        print "\n*** my_frame:\n", my_frame.inspect(my_frame.row_count)

        your_frame.add_columns(complete_data, [
            ("b", str),
            ("c", ia.float64),  # should be int64; DPAT-441
            ("d", str)])
        your_frame.drop_columns('idnum')
        your_frame.sort('b')
        print "\n*** your_frame:\n", your_frame.inspect(your_frame.row_count)

        our_frame = my_frame.join(your_frame, 'b', how='left')
        if 'b' in [_[0] for _ in our_frame.schema]:
            our_frame.sort('b')
        else:
            our_frame.sort('b_L')
        print "\n*** our_frame:\n", our_frame.inspect()
        our_frame_take = our_frame.take(our_frame.row_count,
                                        columns=['a', 'b', 'c_L', 'c_R', 'd'])
        print our_frame_take

        self.assertEqual(our_frame_take, expected_take)

    def test_join_outer(self):
        """
        Outer join example
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(4))
        your_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        your_frame.filter(lambda (row): row.idnum in range(4, 7))

        block_data = [
            # my_frame, rows 0-3
            ("alligator", "bear", "cat"),
            ("auto", "bus", "car"),
            ("apple", "berry", "cantaloupe"),
            ("mirror", "frog", "ball"),
            # your_frame, rows 4-6
            ("bus", 871, "dog"),
            ("berry", 5218, "frog"),
            ("blue", 0, "log")]
        # Correct output
        expected_take = [[u'alligator', u'bear', u'cat', None, None],
                         [u'apple', u'berry', u'cantaloupe', 5218.0, u'frog'],
                         [None, u'blue', None, 0.0, u'log'],
                         [u'auto', u'bus', u'car', 871.0, u'dog'],
                         [u'mirror', u'frog', u'ball', None, None]]
        # Output given under https://intel-data.atlassian.net/browse/DPAT-439
        # expected_take = [[None, None, None, u'blue', 0.0, u'log'],
        #                  [u'alligator', u'bear', u'cat', None, None, None],
        #                  [u'apple', u'berry', u'cantaloupe', u'berry',
        #                   5218.0, u'frog'],
        #                  [u'auto', u'bus', u'car', u'bus', 871.0, u'dog'],
        #                  [u'mirror', u'frog', u'ball', None, None, None]]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [
            ("a", str),
            ("b", str),
            ("c", str)])
        my_frame.drop_columns('idnum')
        my_frame.sort('b')
        print "\n*** my_frame:\n", my_frame.inspect(my_frame.row_count)

        your_frame.add_columns(complete_data, [
            ("b", str),
            ("c", ia.float64),  # should be int64; DPAT-441
            ("d", str)])
        your_frame.drop_columns('idnum')
        your_frame.sort('b')
        print "\n*** your_frame:\n", your_frame.inspect(your_frame.row_count)

        our_frame = my_frame.join(your_frame, 'b', how='outer')
        if 'b' in [_[0] for _ in our_frame.schema]:
            our_frame.sort('b')
        else:
            our_frame.sort('b_L')
        print "\n*** our_frame:\n", our_frame.inspect()

        our_frame_take = our_frame.take(our_frame.row_count,
                                        columns=['a', 'b', 'c_L', 'c_R', 'd'])
        print "\n*** our_frame_take:\n", our_frame_take

        self.assertEqual(our_frame_take, expected_take)

    def test_join_inner(self):
        """
        Inner join example
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(4))
        your_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        your_frame.filter(lambda (row): row.idnum in range(4, 7))

        block_data = [
            # my_frame, rows 0-3
            ("alligator", "bear", "cat"),
            ("auto", "bus", "car"),
            ("apple", "berry", "cantaloupe"),
            ("mirror", "frog", "ball"),
            # your_frame, rows 4-6
            ("bus", 871, "dog"),
            ("berry", 5218, "frog"),
            ("blue", 0, "log")]
        # Correct output
        expected_take = [[u'apple', u'berry', u'cantaloupe', 5218.0, u'frog'],
                         [u'auto', u'bus', u'car', 871.0, u'dog']]
        # Output given under https://intel-data.atlassian.net/browse/DPAT-439
        # expected_take = [[u'apple', u'berry', u'cantaloupe', u'berry',
        #                   5218.0, u'frog'],
        #                  [u'auto', u'bus', u'car', u'bus', 871.0, u'dog']]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [
            ("a", str),
            ("b", str),
            ("c", str)])
        my_frame.drop_columns('idnum')
        my_frame.sort('b')
        print "\n*** my_frame:\n", my_frame.inspect(my_frame.row_count)

        your_frame.add_columns(complete_data, [
            ("b", str),
            ("c", ia.float64),  # should be int64; DPAT-441
            ("d", str)])
        your_frame.drop_columns('idnum')
        your_frame.sort('b')
        print "\n*** your_frame:\n", your_frame.inspect(your_frame.row_count)

        our_frame = my_frame.join(your_frame, 'b', how='inner')
        if 'b' in [_[0] for _ in our_frame.schema]:
            our_frame.sort('b')
        else:
            our_frame.sort('b_L')
        print "\n*** our_frame:\n", our_frame.inspect()
        our_frame_take = our_frame.take(our_frame.row_count,
                                        columns=['a', 'b', 'c_L', 'c_R', 'd'])
        print our_frame_take

        self.assertEqual(our_frame_take, expected_take)

    def test_join_self(self):
        """
        Inner join example from ATK 1.0 documentation
        """
        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum in range(4))

        block_data = [
            ("cat", "abc", "abc", "red"),
            ("doc", "abc", "cde", "pur"),
            ("dog", "cde", "cde", "blk"),
            ("ant", "def", "def", "blk")
        ]
        expected_join = [
            ["cat", "abc", "abc", "red", "cat", "abc", "abc", "red"],
            ["doc", "abc", "cde", "pur", "cat", "abc", "abc", "red"],
            ["dog", "cde", "cde", "blk", "doc", "abc", "cde", "pur"],
            ["dog", "cde", "cde", "blk", "dog", "cde", "cde", "blk"],
            ["ant", "def", "def", "blk", "ant", "def", "def", "blk"]
        ]

        def complete_data(row):
            return block_data[row.idnum]

        my_frame.add_columns(complete_data, [
            ("a", str),
            ("b", str),
            ("book", str),
            ("other", str)])
        my_frame.drop_columns('idnum')
        print my_frame.inspect(my_frame.row_count)

        joined_frame = my_frame.join(my_frame,
                                     left_on='b',
                                     right_on='book',
                                     how='inner')
        print joined_frame.inspect()
        join_take = joined_frame.take(
            joined_frame.row_count,
            columns=['a_L', 'b_L', 'book', 'other_L', 'a_R', 'b_R', 'other_R'])
        # Sorting is allowed as join does not have to preserve order
        self.assertEqual(expected_join.sort(), join_take.sort())

    def test_sorted_k(self):
        """
        sorted_k example enhanced 31 Aug 2015
        """
        frame = frame_utils.build_frame(
            self.example_data, self.example_schema)

        block_data = [
            ("Drama",     1957, "12 Angry Men"),
            ("Crime",     1946, "The Big Sleep"),
            ("Western",   1969, "Butch Cassidy and the Sundance Kid"),
            ("Drama",     1971, "A Clockwork Orange"),
            ("Drama",     2008, "The Dark Knight"),
            ("Animation", 2013, "Frozen"),
            ("Drama",     1972, "The Godfather"),
            ("Animation", 1994, "The Lion King"),
            ("Animation", 2010, "Tangled"),
            ("Fantasy",   1939, "The Wonderful Wizard of Oz")
        ]
        expected_top3 = [
            [u"Animation", 2013, u"Frozen"],
            [u"Animation", 2010, u"Tangled"],
            [u"Drama",     2008, u"The Dark Knight"]
        ]
        expected_top5 = [
            [u"Animation", 2013, u"Frozen"],
            [u"Animation", 2010, u"Tangled"],
            [u"Animation", 1994, u"The Lion King"],
            [u"Crime",     1946, u"The Big Sleep"],
            [u"Drama",     2008, u"The Dark Knight"]
        ]
        expected_top5_age = [
            [u"Animation", 1994, u"The Lion King"],
            [u"Animation", 2010, u"Tangled"],
            [u"Animation", 2013, u"Frozen"],
            [u"Crime",     1946, u"The Big Sleep"],
            [u"Drama",     1957, u"12 Angry Men"]
        ]

        def complete_data(row):
            return block_data[row.idnum]

        frame.filter(lambda (row): row.idnum in range(len(block_data)))
        frame.add_columns(complete_data, [
            ("genre", str),
            ("year", ia.int32),
            ("title", str)
        ])
        frame.drop_columns('idnum')
        print frame.inspect(frame.row_count)

        pick = 3
        topk_frame = frame.sorted_k(pick, [('year', False)])   # DPAT-627
        self.assertEqual(pick, topk_frame.row_count)
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top3, topk_take)
        print topk_frame.inspect(topk_frame.row_count)

        pick = 5
        topk_frame = frame.sorted_k(pick, [('genre', True), ('year', False)])
        self.assertEqual(pick, topk_frame.row_count)
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top5, topk_take)
        print topk_frame.inspect(topk_frame.row_count)

        pick = 5
        topk_frame = frame.sorted_k(
            pick, [('genre', True), ('year', True)], reduce_tree_depth=1)
        self.assertEqual(pick, topk_frame.row_count)
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top5_age, topk_take)
        print topk_frame.inspect(topk_frame.row_count)

    def test_tally(self):
        """
        tally and tally_percent examples
        """
        def complete_data(row):
            return row.idnum % 3

        expected_take = [[0, 0, 0, 0.0],
                         [1, 1, 1, 0.5],
                         [2, 2, 1, 0.5],
                         [3, 0, 1, 0.5],
                         [4, 1, 2, 1.0],
                         [5, 2, 2, 1.0]]

        my_frame = frame_utils.build_frame(
            self.example_data, self.example_schema)
        my_frame.filter(lambda (row): row.idnum < 6)
        my_frame.add_columns(complete_data, ("obs", ia.int32))

        my_frame.sort("idnum")
        print "INITIAL DATA\n", my_frame.inspect()
        my_frame.tally('obs', '1')
        print "AFTER TALLY\n", my_frame.inspect()    # added
        my_frame.sort("idnum")
        print "BEFORE PCT\n", my_frame.inspect()    # added
        my_frame.tally_percent('obs', '1')
        my_frame.sort("idnum")
        print "AFTER PCT & RE-SORT\n", my_frame.inspect()

        tally_take = [x[:3] + [round(x[3], 3)]
                      for x in my_frame.take(my_frame.row_count)]
        print tally_take

        self.assertEqual(expected_take, tally_take)

if __name__ == '__main__':
    unittest.main()
