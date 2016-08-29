"""Run a variety of tests on frame, mostly pulled from doc tests"""

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test


class TestDocExamples(sparktk_test.SparkTKTestCase):

    bin_col_data = self.get_file("bin_col_example.csv")
    bin_col_schema = [("a", int)]

    def test_bin_column_example(self):
        """Test bin columns"""
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

        my_frame = self.context.frame.create(self.bin_col_data, schema=self.bin_col_schema)

        my_frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
                            strict_binning=True, bin_column_name='bin1')
        my_frame.bin_column('a', [5, 12, 25, 60], include_lowest=True,
                            strict_binning=False, bin_column_name='bin2')
        my_frame.bin_column('a', [1, 5, 34, 55, 89], include_lowest=True,
                            strict_binning=False, bin_column_name='bin3')
        my_frame.bin_column('a', [1, 5, 34, 55, 89], include_lowest=False,
                            strict_binning=True, bin_column_name='bin4')

        self.assertItemsEqual(expected, my_frame.take(my_frame.row_count))

    def test_bin_column_eq_example(self):
        """Test bin column equal width/depth"""
        my_frame = self.context.frame.import_csv(
            self.bin_col_data, schema=self.bin_col_schema)
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

        frame_take = my_frame.take(my_frame.row_count)

        for i, j in zip(expected_ed, cutoffs_ed):
            self.assertAlmostEqual(i, j, delta=1.0)
        for i, j in zip(expected_ew, cutoffs_ew):
            self.assertAlmostEqual(i, j, delta=1.0)

        self.assertItemsEqual(expected_take, frame_take)

    def test_classification_metrics_example(self):
        """ classification_metrics example"""
        classification_data = self.get_file("classification_example.csv")
        classification_schema = [("a", str),
                                 ("b", int),
                                 ("labels", int),
                                 ("predictions", int)]

        frame = self.context.frame.import_csv(
            classification_data, schema=classification_schema, header=True)

        cm = frame.classification_metrics(
            label_column='labels', pred_column='predictions',
            pos_label=1, beta=1)

        self.assertEqual(2/3.0, cm.f_measure)
        self.assertEqual(1/2.0, cm.recall)
        self.assertEqual(3/4.0, cm.accuracy)
        self.assertEqual(1/1.0, cm.precision)

    def test_correlation(self):
        """Test correlation and correlation_matrix"""
        c3 = 0.9486932
        my_frame = self.context.frame.create(
            [[0, 1, 4, 0, -1],
             [1, 2, 3, 0, -1],
             [2, 3, 2, 1, -1],
             [3, 4, 1, 2, -1],
             [4, 5, 0, 2, -1]],
            [("x0", float),
             ("x1", float),
             ("x2", float),
             ("x3", float),
             ("x4", float)])

        expected_take = [[1, 1, -1, c3, 0],
                         [1, 1, -1, c3, 0],
                         [-1, -1, 1, -c3, 0],
                         [c3, c3, -c3, 1, 0],
                         [0, 0, 0, 0, 1]]

        corr12 = my_frame.correlation(["x1", "x2"])
        self.assertAlmostEqual(-1.00, corr12)
        corr14 = my_frame.correlation(["x1", "x4"])
        self.assertAlmostEqual(+0.00, corr14)
        corr23 = my_frame.correlation(["x2", "x3"])
        self.assertAlmostEqual(-c3, corr23, delta=.001)

        corr_matrix = my_frame.correlation_matrix(my_frame.column_names)
        corr_take = corr_matrix.take(corr_matrix.row_count)
        for i, j in zip(expected_take, corr_take):
            for h, l in zip(i, j):
                self.assertAlmostEqual(h, l, delta=.001)

    def test_covar_example_cols(self):
        """Test covariance_matrix"""
        covar_cols_data = self.get_file("covar_example_cols.csv")
        covar_cols_schema = [("col_0", int),
                             ("col_1", int),
                             ("col_2", float)]
        my_frame1 = self.context.frame.import_csv(covar_cols_data, schema=covar_cols_schema)
        expected_covar_take = [[1.00, 1.00, -6.65],
                               [1.00, 1.00, -6.65],
                               [-6.65, -6.65, 139.99]]

        cov_matrix = my_frame1.covariance_matrix(['col_0', 'col_1', 'col_2'])
        cov_take = cov_matrix.take(cov_matrix.row_count)
        for i, j in zip(expected_covar_take, cov_take):
            for h, l in zip(i, j):
                self.assertAlmostEqual(h, l, delta=.01)

    def test_covar_example_vector(self):
        """Test covariance_matrix with vector input """
        vector_data = [[[0.0, 1.0, 0.0, 0.0]],
                       [[0.0, 1.0, 0.0, 0.0]],
                       [[0.0, 0.54, 0.46, 0.0]],
                       [[0.0, 0.83, 0.17, 0.0]]]

        my_frame2 = self.context.frame.create(
            vector_data, [('Population_HISTOGRAM', ia.vector(4))])

        expected_covar_take = [[0, 0.00000000, 0.00000000, 0],
                               [0, 0.04709167, -0.04709167, 0],
                               [0, -0.04709167, 0.04709167, 0],
                               [0, 0.00000000, 0.00000000, 0]]

        covar_matrix = my_frame2.covariance_matrix(['Population_HISTOGRAM'])
        covar_take = covar_matrix.take(covar_matrix.row_count)

        for i, j in zip(expected_covar_take, covar_take):
            for h, l in zip(i, j[0]):
                self.assertAlmostEqual(h, l, delta=.01)

    def test_cumu_example(self):
        """cumulative_sum and cumulative_percent"""
        my_frame = self.context.frame.create(
            [[0], [1], [2], [0], [1], [2]], [("obs", int)])

        expected_sum = [0, 1, 3, 3, 4, 6]
        expected_percent = [0.0, .1667, .5, .5, .6667, 1.0]

        my_frame.cumulative_sum('obs')
        my_frame.cumulative_percent('obs')
        pd_frame = my_frame.download(my_frame.row_count)

        for i, j in pd_frame.iterrows():
            self.assertEqual(expected_sum[i], j['obs_cumulative_sum'])
            self.assertAlmostEqual(
                expected_percent[i], j['obs_cumulative_percent'], delta=.001)

    def test_drop_duplicates(self):
        """drop_duplicates examples in sequence"""
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

        my_frame = self.context.frame.create(
            frame_load, schema=[("a", int), ("b", int), ("c", int)])

        self.assertEqual(my_frame.row_count, 11)
        my_frame.drop_duplicates()
        self.assertEqual(my_frame.row_count, 7)
        my_frame.drop_duplicates(["a", "c"])
        self.assertEqual(my_frame.row_count, 4)
        my_frame.drop_duplicates("b")
        self.assertLessEqual(my_frame.row_count, 3)  # Either 2 or 3 rows

    def test_dot_product(self):
        """dot_product both column and vector"""
        frame_data = [[1, 0.2, -2, 5, [1.0, 0.2], [-2.0, 5.0]],
                      [2, 0.4, -1, 6, [2.0, 0.4], [-1.0, 6.0]],
                      [3, 0.6, 0, 7, [3.0, 0.6], [0.0, 7.0]],
                      [4, 0.8, 1, 8, [4.0, 0.8], [1.0, 8.0]],
                      [5, None, 2, None, [5.0, None], [2.0, None]]]

        my_frame = self.context.frame.create(
            frame_data, schema=[("col_0", float),
                         ("col_1", float),
                         ("col_2", int),
                         ("col_3", int),
                         ("col_4", vector(2)),
                         ("col_5", vector(2))])

        my_frame.dot_product(
            ['col_0', 'col_1'], ['col_2', 'col_3'], 'dot_product')
        my_frame.dot_product(
            ['col_0', 'col_1'], ['col_2', 'col_3'], 'dot_product_2',
            [0.1, 0.2], [0.3, 0.4])
        my_frame.dot_product('col_4', 'col_5', 'dot_product_v')

        dot_product = [-1, 0.4, 4.2, 10.4, 10.0]

        pd_frame = my_frame.download(my_frame.row_count)
        for i, j in pd_frame.iterrows():
            self.assertAlmostEqual(j['dot_product'], dot_product[i], delta=0.1)
            self.assertAlmostEqual(
                j['dot_product_2'], dot_product[i], delta=0.1)
            self.assertAlmostEqual(
                j['dot_product_v'], dot_product[i], delta=0.1)

    def test_flatten(self):
        """ flatten_column example unflatten_column example """
        block_data = [[1, "solo,mono,single"], [2, "duo,double"]]
        my_frame = self.context.frame.create(
            block_data, schema=[('a', int), ('b', str)])
        # frame after flatten
        expected_prod_take = [[1, "mono"],
                              [1, "single"],
                              [1, "solo"],
                              [2, "double"],
                              [2, "duo"]]

        # frame after unflatten: note that strings are alpha-sorted.
        expected_unflatten_take = [[1, ['mono', 'single', 'solo']],
                                   [2, ['double', 'duo']]]

        my_frame.flatten_columns('b')
        my_frame_take = my_frame.take(my_frame.row_count)
        self.assertItemsEqual(expected_prod_take, my_frame_take)

        my_frame.unflatten_columns(['a'])
        my_frame_take = my_frame.take(my_frame.row_count)
        results = [[i[0], sorted(i[1].split(','))] for i in my_frame_take]
        self.assertItemsEqual(results, expected_unflatten_take)

    def test_group_by_agg(self):
        """test group_by count"""
        block_data = [["cat"], ["apple"], ["bat"], ["cat"], ["bat"], ["cat"]]
        my_frame = self.context.frame.create(block_data, schema=[('a', str)])

        expected_prod_take = [["apple", 1], ["bat", 2], ["cat", 3]]

        new_frame = my_frame.group_by('a', ia.agg.count)
        new_frame_take = new_frame.take(new_frame.row_count)
        self.assertItemsEqual(expected_prod_take, new_frame_take)

    #def test_group_by_avg(self):
    #    """test group_by average"""
    #    block_data = [[1, "alpha", 3.0],
    #                  [1, "bravo", 5.0],
    #                  [1, "alpha", 5.0],
    #                  [2, "bravo", 8.0],
    #                  [2, "bravo", 12.0]]
    #    expected_prod_take = [[1, "alpha", 4.0],
    #                          [1, "bravo", 5.0],
    #                          [2, "bravo", 10.0]]
    #
    #    my_frame = self.context.frame.create(
    #        block_data, schema=[("a", int), ("b", str), ("c", float)])
    #
    #    new_frame = my_frame.group_by(['a', 'b'], {'c': ia.agg.avg})
    #    new_frame_take = new_frame.take(new_frame.row_count)
    #    self.assertItemsEqual(expected_prod_take, new_frame_take)

    #def test_group_by_stats(self):
    #    """group_by, several attributes checked"""
    #    block_data = [["ape", 1, 4.0, 9], ["ape", 1, 8.0, 8],
    #                  ["big", 1, 5.0, 7], ["big", 1, 6.0, 6],
    #                  ["big", 1, 8.0, 5]]
    #
    #    my_frame = ia.Frame(ia.UploadRows(
    #        block_data, [("a", str), ("c", ia.int32),
    #                     ("d", ia.float64), ("e", ia.int32)]))
    #
    #    expected_take = {u'a': {0: u'ape', 1: u'big'},
    #                     u'count': {0: 2, 1: 3},
    #                     u'c': {0: 1, 1: 1},
    #                     u'd_SUM': {0: 12.0, 1: 19.0},
    #                     u'd_MIN': {0: 4.0, 1: 5.0},
    #                     u'd_AVG': {0: 6.0, 1: 6.333333333333333},
    #                     u'e_MAX': {0: 9, 1: 7}}
    #
    #    new_frame = my_frame.group_by(
    #        ['a', 'c'], ia.agg.count,
    #        {'d': [ia.agg.avg, ia.agg.sum, ia.agg.min], 'e': ia.agg.max})
    #    new_frame.sort('a')
    #    new_res = new_frame.download(new_frame.row_count)
    #    self.assertEqual(new_res.to_dict(), expected_take)

    def test_histogram(self):
        """test histogram"""
        block_data = [["a", 2], ["b", 7], ["c", 3], ["d", 9], ["e", 1]]
        frame = self.context.frame.create(
            block_data, schema=[("a", str), ("b", int)])

        expected_width_cutoffs = [1.0, 1 + 8.0/3, 1 + 2*8.0/3, 9.0]
        expected_width_hist = [3, 0, 2]
        expected_width_density = [0.6, 0.0, 0.4]

        expected_depth_cutoffs = [1, 2, 7, 9]
        expected_depth_hist = [1, 2, 2]
        expected_depth_density = [0.2, 0.4, 0.4]

        hist = frame.histogram("b", num_bins=3)
        self.assertEqual(expected_width_cutoffs, hist.cutoffs)
        self.assertEqual(expected_width_hist, hist.hist)
        self.assertEqual(expected_width_density, hist.density)

        hist = frame.histogram("b", num_bins=3, bin_type='equaldepth')
        self.assertEqual(expected_depth_cutoffs, hist.cutoffs)
        self.assertEqual(expected_depth_hist, hist.hist)
        self.assertEqual(expected_depth_density, hist.density)

    def _build_join_frames(self):
        my_data = [["alligator", "bear", "cat"],
                   ["auto", "bus", "car"],
                   ["apple", "berry", "cantaloupe"],
                   ["mirror", "frog", "ball"]]

        your_data = [["bus", 871, "dog"],
                     ["berry", 5218, "frog"],
                     ["blue", 0, "log"]]

        my_frame = self.context.frame.create(
            my_data, schema=[('a', str), ('b', str), ('c', str)])
        your_frame = self.context.frame.create(
            your_data, schema=[('b', str), ('c', int), ('d', str)])
        return (my_frame, your_frame)

    def test_join_left(self):
        """test left join"""
        (my_frame, your_frame) = self._build_join_frames()

        expected_take = [[u'alligator', u'bear', u'cat', None, None],
                         [u'apple', u'berry', u'cantaloupe', 5218, u'frog'],
                         [u'auto', u'bus', u'car', 871.0, u'dog'],
                         [u'mirror', u'frog', u'ball', None, None]]

        our_frame = my_frame.join(your_frame, 'b', how='left')
        our_frame_take = our_frame.take(
            our_frame.row_count, columns=['a', 'b_L', 'c_L', 'c_R', 'd'])

        self.assertItemsEqual(our_frame_take, expected_take)

    def test_join_outer(self):
        """ Outer join example """
        (my_frame, your_frame) = self._build_join_frames()

        expected_take = [[u'alligator', u'bear', u'cat', None, None],
                         [u'apple', u'berry', u'cantaloupe', 5218.0, u'frog'],
                         [None, u'blue', None, 0.0, u'log'],
                         [u'auto', u'bus', u'car', 871.0, u'dog'],
                         [u'mirror', u'frog', u'ball', None, None]]

        our_frame = my_frame.join(your_frame, 'b', how='outer')
        our_frame_take = our_frame.take(
            our_frame.row_count, columns=['a', 'b_L', 'c_L', 'c_R', 'd'])

        self.assertItemsEqual(our_frame_take, expected_take)

    def test_join_inner(self):
        """ Inner join example """
        (my_frame, your_frame) = self._build_join_frames()
        expected_take = [[u'apple', u'berry', u'cantaloupe', 5218, u'frog'],
                         [u'auto', u'bus', u'car', 871, u'dog']]

        our_frame = my_frame.join(your_frame, 'b', how='inner')
        our_frame_take = our_frame.take(our_frame.row_count,
                                        columns=['a', 'b', 'c_L', 'c_R', 'd'])

        self.assertItemsEqual(our_frame_take, expected_take)

    def test_join_self(self):
        """ Inner join example from ATK 1.0 documentation """
        block_data = [["cat", "abc", "abc", "red"],
                      ["doc", "abc", "cde", "pur"],
                      ["dog", "cde", "cde", "blk"],
                      ["ant", "def", "def", "blk"]]
        expected_join = [["cat", "abc", "abc", "red", "cat", "abc", "red"],
                         ["doc", "abc", "cde", "pur", "cat", "abc", "red"],
                         ["dog", "cde", "cde", "blk", "doc", "abc", "pur"],
                         ["dog", "cde", "cde", "blk", "dog", "cde", "blk"],
                         ["ant", "def", "def", "blk", "ant", "def", "blk"]]

        my_frame = self.context.frame.create(
            block_data, schema=[("a", str), ("b", str),
                         ("book", str), ("other", str)])

        joined_frame = my_frame.join(
            my_frame, left_on='b', right_on='book', how='inner')
        join_take = joined_frame.take(
            joined_frame.row_count,
            columns=['a_L', 'b_L', 'book', 'other_L', 'a_R', 'b_R', 'other_R'])
        self.assertItemsEqual(expected_join, join_take)

    def test_sorted_k(self):
        """ sorted_k example enhanced 31 Aug 2015 """

        block_data = [("Drama",     1957, "12 Angry Men"),
                      ("Crime",     1946, "The Big Sleep"),
                      ("Western",   1969, "Butch Cassidy"),
                      ("Drama",     1971, "A Clockwork Orange"),
                      ("Drama",     2008, "The Dark Knight"),
                      ("Animation", 2013, "Frozen"),
                      ("Drama",     1972, "The Godfather"),
                      ("Animation", 1994, "The Lion King"),
                      ("Animation", 2010, "Tangled"),
                      ("Fantasy",   1939, "The Wonderful Wizard of Oz")]

        expected_top3 = [[u"Animation", 2013, u"Frozen"],
                         [u"Animation", 2010, u"Tangled"],
                         [u"Drama",     2008, u"The Dark Knight"]]

        expected_top5 = [[u"Animation", 2013, u"Frozen"],
                         [u"Animation", 2010, u"Tangled"],
                         [u"Animation", 1994, u"The Lion King"],
                         [u"Crime",     1946, u"The Big Sleep"],
                         [u"Drama",     2008, u"The Dark Knight"]]

        expected_top5_age = [[u"Animation", 1994, u"The Lion King"],
                             [u"Animation", 2010, u"Tangled"],
                             [u"Animation", 2013, u"Frozen"],
                             [u"Crime",     1946, u"The Big Sleep"],
                             [u"Drama",     1957, u"12 Angry Men"]]

        frame = self.context.frame.create(
            block_data, schema=[("genre", str), ("year", int), ("title", str)])

        topk_frame = frame.sorted_k(3, [('year', False)])
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top3, topk_take)

        topk_frame = frame.sorted_k(5, [('genre', True), ('year', False)])
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top5, topk_take)

        topk_frame = frame.sorted_k(
            5, [('genre', True), ('year', True)], reduce_tree_depth=1)
        topk_take = topk_frame.take(topk_frame.row_count)
        self.assertEqual(expected_top5_age, topk_take)

    def test_tally(self):
        """tally and tally_percent examples"""
        data = [[0, 0], [1, 1], [2, 2], [3, 0], [4, 1], [5, 2]]
        expected_tally = [0, 1, 1, 1, 2, 2]
        expected_percent = [0.0, 0.5, 0.5, 0.5, 1.0, 1.0]

        my_frame = self.context.frame.create(
            data, schema=[('idnum', int), ('obs', int)])

        my_frame.tally('obs', '1')
        my_frame.tally_percent('obs', '1')

        pd_frame = my_frame.download(my_frame.row_count)

        for i, j in pd_frame.iterrows():
            self.assertEqual(expected_tally[i], j['obs_tally'])
            self.assertAlmostEqual(expected_percent[i], j['obs_tally_percent'])


if __name__ == '__main__':
    unittest.main()
