'''Test that Frame statistic methods'''

import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.realpath(__file__)))))
from qalib import sparktk_test
from sparktk import dtypes


class FrameStatsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameStatsTest, self).setUp()
        self.schema = [("weight", float), ("item", str)]
        self.stat_frame = self.context.frame.import_csv(
            self.get_file("mode_stats.tsv"),
            schema=self.schema,
            delimiter='\t')

    def test_column_mode(self):
        """Validate column mode"""
        stats = self.stat_frame.column_mode(
            "item", "weight", max_modes_returned=50)
        expected_mode = {'Poliwag', 'Pumpkaboo', 'Scrafty',
                         'Psyduck', 'Alakazam'}

        self.assertEqual(stats.mode_count, 5)
        self.assertEqual(stats.total_weight, 1749)
        self.assertEqual(set(stats.modes), expected_mode)

    def test_column_median(self):
        """Validate column median without weight"""
        schema = [("item", float)]
        frame = self.context.frame.import_csv(
            self.get_file("weight_median.csv"), schema=schema)
        stats = frame.column_median("item")
        self.assertEqual(stats, 499)

    def test_column_bad_input_median(self):
        """Validate column median for bad input"""
        schema = [("item", str)]
        frame = self.context.frame.create(
            [['Psyduck'], ['Balatoise'], ['Slowbro']],
            schema=schema)
        with self.assertRaisesRegexp(
                Exception, "Could not parse .* as a Double"):
            stats = frame.column_median("item")

    def test_summary_statistics(self):
        """Valiadate results of summary statistics"""
        schema = [("item", float)]
        expected_stats = [369.12360275372976,
                          999,
                          999.0,
                          482.1077297881646,
                          517.8922702118355,
                          1.0,
                          288.5307609250702]
        stat_frame = self.context.frame.import_csv(
            self.get_file("summary_stats.csv"),
            schema=schema)
        stats = stat_frame.column_summary_statistics("item")
        self.assertItemsEqual([stats.geometric_mean,
                               stats.good_row_count,
                               stats.maximum,
                               stats.mean_confidence_lower,
                               stats.mean_confidence_upper,
                               stats.minimum,
                               stats.standard_deviation], expected_stats)

if __name__ == "__main__":
    unittest.main()
