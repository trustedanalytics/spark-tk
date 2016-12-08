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

""" Exercise NaN and Inf values in various contexts.  """
import unittest
import numpy as np
import math
import sys
import os
from sparktkregtests.lib import sparktk_test


class ExtremeValueTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        self.data_proj = self.get_file("extreme_value.csv")
        self.schema_proj = [("col_A", int),
                            ("col_B", int),
                            ("col_C", float),
                            ("Double", float),
                            ("Text", str)]

    def test_extreme_projection(self):
        """ Test projection including Inf / NaN data """
        master = self.context.frame.import_csv(
            self.data_proj, schema=self.schema_proj)
        self.assertEqual(master.count(), 7)

        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {123456: np.inf,
                       777: -np.inf,
                       4321: np.nan}
            return new_val.get(row["col_A"], row["col_A"])

        master.add_columns(add_extremes, ("col_D", float))
        
        proj_3col = master.copy(['col_D', 'Double', 'Text'])
        self.assertEqual(proj_3col.count(), master.count())
        self.assertEqual(len(proj_3col.column_names), 3)

        proj_1col = master.copy({'col_A': 'extremes'})
        self.assertEqual(proj_1col.count(), master.count())
        self.assertEqual(len(proj_1col.column_names), 1)

        #check if NaN/inf values are present
        test_extreme = master.to_pandas()
        for index, row in test_extreme.iterrows():
            if(row['col_A'] == 123456 or row['col_A'] == 777):
                self.assertTrue(math.isinf(row['col_D']))
            if(row['col_A'] == 4321):
                self.assertTrue(math.isnan(row['col_D']))

    def test_extreme_copy(self):
        """ Test copy with Inf / NaN data """
        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {700: np.inf,
                       701: -np.inf,
                       702: np.nan}
            return new_val.get(row["col_A"], row["col_A"])

        frame = self.context.frame.import_csv(
            self.data_proj, schema=self.schema_proj)
        frame.add_columns(add_extremes, ("col_D", float))

        frame_copy = frame.copy()
        self.assertEqual(frame.column_names, frame_copy.column_names)
        self.assertFramesEqual(frame_copy, frame)

    def test_extreme_maxmin32(self):
        """ Test extremal 32 bit float values"""
        schema_maxmin32 = [("col_A", float),
                           ("col_B", float)]
        extreme32 = self.context.frame.import_csv(
            self.get_file("BigAndTinyFloat32s.csv"),
            schema=schema_maxmin32)
        self.assertEqual(extreme32.count(), 16)
        extreme32.add_columns(lambda row: [np.sqrt(-9)],
                              [('neg_root', float)])
        extake = extreme32.to_pandas(extreme32.count())
        for index, row in extake.iterrows():
            self.assertTrue(math.isnan(row['neg_root']))

    def test_extreme_maxmin64(self):
        """ Test extreme large and small magnitudes on 64-bit floats."""
        data_maxmin64 = self.get_file('BigAndTinyFloat64s.csv')
        schema_maxmin64 = [("col_A", float),
                           ("col_B", float)]
        extreme64 = self.context.frame.import_csv(
            data_maxmin64, schema=schema_maxmin64)
        self.assertEqual(extreme64.count(), 16)

        extreme64.add_columns(lambda row:
                              [row.col_A*2, np.sqrt(-9)],
                              [("twice", float),
                               ('neg_root', float)])
        extake = extreme64.to_pandas(extreme64.count())

        #check for inf when values exceed 64-bit range;
        #double the value if outside the range [0,1)
        for index, row in extake.iterrows():
            if row['col_A'] >= 1 or row['col_A'] < 0:
                self.assertTrue(math.isinf(row['twice']))
            else:
                self.assertEqual(row['twice'], row['col_A'] * 2)
            self.assertTrue(math.isnan(row['neg_root']))

    def test_extreme_colmode(self):
        """ Insert NaN and +/-Inf for weights"""
        def add_extremes(row):
            new_val = {"Charizard": np.inf,
                       "Squirtle": -np.inf,
                       "Wartortle": np.nan}
            return new_val.get(row["item"], row['weight'])

        data_stat = self.get_file("mode_stats.tsv")
        schema_stat = [("weight", float), ("item", int)]
        stat_frame = self.context.frame.import_csv(
            data_stat, schema=schema_stat, delimiter="\t")
        stat_frame.add_columns(add_extremes, ("weight2", float))
        stats = stat_frame.column_mode(
            "item", "weight2", max_modes_returned=50)

        self.assertEqual(stats.mode_count, 2)
        self.assertEqual(stats.total_weight, 1749)
        self.assertIn(60 , stats.modes)

    def test_extreme_col_summary(self):
        """ Test column_summary_stats with Inf / NaN data """
        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {20: np.inf,
                       30: -np.inf,
                       40: np.nan}
            return new_val.get(row.Item, row.Item)

        data_mode = self.get_file("mode_stats2.csv")
        schema_mode = [("Item", int), ("Weight", int)]
        stat_frame = self.context.frame.import_csv(
            data_mode, schema=schema_mode)

        # Create new column where only values 10 and 50 are valid;
        stat_frame.add_columns(add_extremes, ("Item2", float))
        stat_frame.drop_columns("Item")
        stat_frame.rename_columns({"Item2": "Item"})

        stats = stat_frame.column_summary_statistics(
                           "Item",
                           weights_column="Weight",
                           use_popultion_variance=True)

        self.assertEqual(stats.mean, 30)
        self.assertEqual(stats.good_row_count, 20)
        self.assertAlmostEqual(stats.mean_confidence_upper, 34.3826932359)

    def test_extreme_col_summary_corner(self):
        """ Test column_summary_stats with very large values and odd cases."""
        def add_extremes(row):
            bf = float(2) ** 1021
            new_val = {600: 30,
                       1: 0,
                       2: bf * 2,
                       3: float(2) ** 1023,
                       3.1: 5.4}
            return new_val.get(row.Factors, row.Factors)
        expected_stats = [1624.6, 56, 563700.64]
        data_corner = self.get_file("SummaryStats2.csv")
        schema_corner = [("Item", float), ("Factors", float)]

        stat_frame = self.context.frame.import_csv(
            data_corner, schema=schema_corner)

        stat_frame.add_columns(add_extremes, ("Weight", float))
        stat_frame.drop_columns("Factors")
        stats = stat_frame.column_summary_statistics(
                           "Item", weights_column="Weight")
        self.assertAlmostEqual([stats.mean,
                               stats.good_row_count,
                               stats.variance], expected_stats)

    def test_extreme_col_summary_empty_frame(self):
        expected_stats = [float('nan'), 0, 1.0]
        schema = [("Item", float), ("Weight", float)]
        stat_frame = self.context.frame.create([], schema=schema)
        stats = stat_frame.column_summary_statistics(
                           "Item", weights_column="Weight")
        self.assertTrue([math.isnan(stats.maximum),
                         stats.good_row_count,
                         stats.geometric_mean], expected_stats)

if __name__ == "__main__":
    unittest.main()
