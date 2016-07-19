""" Test Binning against a separate implemenation """
import unittest
import sys
import os
from spartkregtests.lin import sparktk_test


class BinningHarness(spartk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frames"""
        super(BinningHarness, self).setUp()
        self.schema = [("user_id", int),
                       ("vertex_type", str),
                       ("movie_id", int),
                       ("rating", int),
                       ("splits", str)]

        # Movie user data with original ratings
        # Big data frame from data with 5 ratings
        self.frame_5ratings = self.context.frame.import_csv(
            self.get_file("movie_user_5ratings.csv"), schema=self.schema)

        # Movie user data with some missing ratings
        # Big data frame from data with only 1 ratings
        self.frame_1ratings = self.context.frame.import_csv(
            self.get_file("movie_user_1rating.csv"), schema=self.schema)

    def test_equalwidth_5bins_5ratings(self):
        """Equal width binning on 5 ratings into 5 bin"""
        baseline = [[0, 3926L], [1, 3974L], [2, 3792L], [3, 3988L], [4, 3922L]]
        self.frame_5ratings.bin_column("rating", 5, bin_column_name="binned0")
        groupby_frame = self.frame_5ratings.group_by(
            "binned0", self.context.agg.count)
        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equalwidth_2bins_5ratings(self):
        """Equal width  binning on 5 ratings into 2 bins"""
        baseline = [[0, 7900L], [1, 11702L]]
        self.frame_5ratings.bin_column("rating", 2, bin_column_name="binned1")
        groupby_frame = self.frame_5ratings.group_by(
            "binned1", self.context.agg.count)

        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equalwidth_10bins_5ratings(self):
        """Equal Width binning on 5 ratings and 10 bins"""
        baseline = [[0, 3926L], [2, 3974L], [5, 3792L], [7, 3988L], [9, 3922L]]
        self.frame_5ratings.bin_column("rating", 10, bin_column_name="binned2")
        groupby_frame = self.frame_5ratings.group_by(
            "binned2", self.context.agg.count)

        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equaldepth_5bins_5ratings(self):
        """Equal depth binning on 5 ratings into 5 bins"""
        baseline = [[0, 3926L], [1, 3974L], [2, 3792L], [3, 3988L], [4, 3922L]]
        self.frame_5ratings.quantile_bin_column(
            "rating", 5, bin_column_name="binned3")
        groupby_frame = self.frame_5ratings.group_by(
            "binned3", self.context.agg.count)

        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equaldepth_2bins_5ratings(self):
        """Equal depth binning on 5 ratings into 2 bins"""
        baseline = [[0, 11692L], [1, 7910L]]
        self.frame_5ratings.quantile_bin_column(
            "rating", 2, bin_column_name="binned4")
        groupby_frame = self.frame_5ratings.group_by(
            "binned4", self.context.agg.count)

        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equaldepth_10bins_5ratings(self):
        """Equal depth binning on 5 ratings into 10 bins """
        baseline = [[0, 3926L], [1, 3974L], [2, 3792L], [3, 3988L], [4, 3922L]]
        self.frame_5ratings.quantile_bin_column(
            "rating", 10, bin_column_name="binned5")
        groupby_frame = self.frame_5ratings.group_by(
            "binned5", self.context.agg.count)

        self.assertItemsEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equalwidth_5bins_1rating(self):
        """Equal width binning on 1 rating and 5 bins"""
        baseline = [[0, 19602L]]
        self.frame_1ratings.bin_column("rating", 5, bin_column_name="binned6")
        groupby_frame = self.frame_1ratings.group_by(
            "binned6", self.context.agg.count)

        self.assertEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_equaldepth_5bins_1rating(self):
        """Equal depth binning on 1 rating and 5 bins"""
        baseline = [[0, 19602L]]
        self.frame_1ratings.quantile_bin_column(
            "rating", 5, bin_column_name="binned7")
        groupby_frame = self.frame_1ratings.group_by(
            "binned7", self.context.agg.count)

        self.assertEqual(sorted(groupby_frame.take(10).data), baseline)
        self.schema = [("user_id", ia.int32),
                       ("vertex_type", str),
                       ("movie_id", ia.int32),
                       ("rating", ia.int32),
                       ("splits", str)]

        netf_5_ratings = cu.get_file("netf_5_ratings.csv")
        netf_all_3 = cu.get_file("netf_all_3.csv")

        # Movie user data with original ratings
        # Big data frame from data with 5 ratings
        self.frame_5ratings = ia.Frame(ia.CsvFile(netf_5_ratings, self.schema))

        # Movie user data with some missing ratings
        # Big data frame from data with only 1 ratings
        self.frame_1ratings = ia.Frame(ia.CsvFile(netf_all_3, self.schema))

    def test_equalwidth_5bins_5ratings(self):
        """Equal width binning on 5 ratings into 5 bin"""
        baseline = [[0, 3970], [1, 3744], [2, 3994], [3, 3950], [4, 3944]]
        self.frame_5ratings.bin_column_equal_width("rating", 5, "binned0")
        groupby_frame = self.frame_5ratings.group_by("binned0", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equalwidth_2bins_5ratings(self):
        """Equal width  binning on 5 ratings into 2 bins"""
        baseline = [[0, 7714], [1, 11888]]
        self.frame_5ratings.bin_column_equal_width("rating", 2, "binned1")
        groupby_frame = self.frame_5ratings.group_by("binned1", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equalwidth_10bins_5ratings(self):
        """Equal Width binning on 5 ratings and 10 bins"""
        baseline = [[0, 3970], [2, 3744], [5, 3994], [7, 3950], [9, 3944]]
        self.frame_5ratings.bin_column_equal_width("rating", 10, "binned2")
        groupby_frame = self.frame_5ratings.group_by("binned2", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equaldepth_5bins_5ratings(self):
        """Equal depth binning on 5 ratings into 5 bins"""
        baseline = [[0, 3970], [1, 3744], [2, 3994], [3, 3950], [4, 3944]]
        self.frame_5ratings.bin_column_equal_depth("rating", 5, "binned3")
        groupby_frame = self.frame_5ratings.group_by("binned3", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equaldepth_2bins_5ratings(self):
        """Equal depth binning on 5 ratings into 2 bins"""
        baseline = [[0, 11708], [1, 7894]]
        self.frame_5ratings.bin_column_equal_depth("rating", 2, "binned4")
        groupby_frame = self.frame_5ratings.group_by("binned4", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equaldepth_10bins_5ratings(self):
        """Equal depth binning on 5 ratings into 10 bins """
        self.frame_5ratings.bin_column_equal_depth("rating", 10, "binned5")
        baseline = [[0, 3970], [1, 3744], [2, 3994], [3, 3950], [4, 3944]]
        groupby_frame = self.frame_5ratings.group_by("binned5", ia.agg.count)
        self.assertItemsEqual(groupby_frame.take(10), baseline)

    def test_equalwidth_5bins_1rating(self):
        """Equal width binning on 1 rating and 5 bins"""
        self.frame_1ratings.bin_column_equal_width("rating", 5, "binned6")
        baseline = [[0, 19602]]
        groupby_frame = self.frame_1ratings.group_by("binned6", ia.agg.count)
        self.assertEqual(sorted(groupby_frame.take(10)), baseline)

    def test_equaldepth_5bins_1rating(self):
        """Equal depth binning on 1 rating and 5 bins"""
        self.frame_1ratings.bin_column_equal_depth("rating", 5, "binned7")
        baseline = [[0, 19602]]
        groupby_frame = self.frame_1ratings.group_by("binned7", ia.agg.count)
        self.assertEqual(sorted(groupby_frame.take(10)), baseline)

    def test_equaldepth_2bins_1_2_5_rating(self):
        """Equal depth binning on ratings 1, 2 and 5 into 2 bins"""
        # Movie user data with some missing ratings
        # Big data frame from data with ratings 1, 2 and 5 only.
        baseline = [[0, 12992L], [1, 6610L]]
        frame_1_2_5_ratings = self.context.frame.import_csv(
            self.get_file("movie_user_3ratings.csv"), schema=self.schema)

        frame_1_2_5_ratings.quantile_bin_column(
            "rating", 2, bin_column_name="binned8")
        groupby_frame = frame_1_2_5_ratings.group_by(
            "binned8", self.context.agg.count)

        self.assertEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_bin_cutoffs_strict_binning_true_4bins_5ratings(self):
        """Binning using cutoffs on 5 ratings and 2 bins"""
        baseline = [[-1, 3922L], [0, 7900L], [1, 3792L], [2, 3988L]]
        self.frame_5ratings.bin_column(
            "rating", [1, 2, 3, 4],
            include_lowest=False,
            strict_binning=True,
            bin_column_name="binned9")
        groupby_frame = self.frame_5ratings.group_by(
            "binned9", self.context.agg.count)

        self.assertEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_bin_cutoff_strict_binning_false_3bins_5ratings(self):
        """Binning using cutoff with strict binning false"""
        baseline = [[0, 7900L], [1, 3792L], [2, 7910L]]
        self.frame_5ratings.bin_column(
            "rating", [1, 2, 3, 4],
            include_lowest=False,
            strict_binning=False,
            bin_column_name="binned10")
        groupby_frame = self.frame_5ratings.group_by(
            "binned10", self.context.agg.count)

        self.assertEqual(sorted(groupby_frame.take(10).data), baseline)

    def test_negative_bin_number(self):
        with self.assertRaises(Exception):
            self.frame_5ratings.bin_column(
            "rating", -1)

if __name__ == '__main__':
    unittest.main()
