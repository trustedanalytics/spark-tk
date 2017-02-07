# vim: set encoding=utf-8

#  Copyright (c) 2016 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

'''Test that Frame statistic methods'''

import unittest
from sparktkregtests.lib import sparktk_test


class FrameStatsTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameStatsTest, self).setUp()
        self.schema = [("weight", float), ("item", int)]
        self.stat_frame = self.context.frame.import_csv(
            self.get_file("mode_stats.tsv"),
            schema=self.schema,
            delimiter='\t')

    def test_column_mode(self):
        """Validate column mode"""
        stats = self.stat_frame.column_mode(
            "item", "weight", max_modes_returned=3)
        expected_mode = {60, 54}

        self.assertEqual(stats.mode_count, 2)
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
            [['Duck'], ['Tortoise'], ['Slug']],
            schema=schema)
        with self.assertRaisesRegexp(
                Exception, "Could not parse .* as a Double"):
            frame.column_median("item")

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
        stats_data = [[i] for i in xrange(1, 1000)]
        stat_frame = self.context.frame.create(
            stats_data,
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
