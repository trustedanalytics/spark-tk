""" Exercise NaN and Inf values in various contexts.  """
import unittest
import numpy as np
import math
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test
from sparktk import dtypes


class ExtremeValueTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        self.data_proj = self.get_file("extreme_value.csv")
        self.schema_proj = [("col_A", dtypes.int32),
                            ("col_B", dtypes.int64),
                            ("col_C", dtypes.float32),
                            ("Double", dtypes.float64),
                            ("Text", str)]

    def test_extreme_projection(self):
        """ Test projection including Inf / NaN data """
        master = self.context.frame.import_csv(
            self.data_proj, schema=self.schema_proj)
        self.assertEqual(master.row_count, 7)

        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {123456: np.inf,
                       777: -np.inf,
                       4321: np.nan}
            return new_val.get(row["col_A"], row["col_A"])

        master.add_columns(add_extremes, ("col_D", dtypes.float64))

        proj_3col = master.copy(['col_D', 'Double', 'Text'])
        self.assertEqual(proj_3col.row_count, master.row_count)
        self.assertEqual(len(proj_3col.column_names), 3)

        proj_1col = master.copy({'col_A': 'extremes'})
        self.assertEqual(proj_1col.row_count, master.row_count)
        self.assertEqual(len(proj_1col.column_names), 1)

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
        frame.add_columns(add_extremes, ("col_D", dtypes.float64))

        frame_copy = frame.copy()
        self.assertEqual(frame.column_names, frame_copy.column_names)
        self.assertFramesEqual(frame_copy, frame)

    def test_extreme_maxmin32(self):
        """ Test extremal 32 bit float values"""
        schema_maxmin32 = [("col_A", dtypes.float32),
                           ("col_B", dtypes.float32)]
        extreme32 = self.context.frame.import_csv(
            self.get_file("BigAndTinyFloat32s.csv"),
            schema=schema_maxmin32)
        self.assertEqual(extreme32.row_count, 16)
        extreme32.add_columns(lambda row: [np.sqrt(-9)],
                              [('neg_root', dtypes.float32)])
        extake = extreme32.download(extreme32.row_count)
        for _, j in extake.iterrows():
            self.assertTrue(math.isnan(j['neg_root']))

    def test_extreme_maxmin64(self):
        """ Test extreme large and small magnitudes on 64-bit floats."""
        data_maxmin64 = self.get_file('BigAndTinyFloat64s.csv')
        schema_maxmin64 = [("col_A", dtypes.float64),
                           ("col_B", dtypes.float64)]
        extreme64 = self.context.frame.import_csv(
            data_maxmin64, schema=schema_maxmin64)
        self.assertEqual(extreme64.row_count, 16)

        extreme64.add_columns(lambda row:
                              [row.col_A*2, np.sqrt(-9)],
                              [("twice", dtypes.float64),
                               ('neg_root', dtypes.float64)])

        extake = extreme64.download(extreme64.row_count)
        for _, j in extake.iterrows():
            if j['col_A'] > 1 or j['col_A'] < 0:
                self.assertTrue(math.isinf(j['twice']))
            else:
                self.assertEqual(j['twice'], j['col_A'] * 2)
            self.assertTrue(math.isnan(j['neg_root']))

    def test_extreme_colmode(self):
        """ Insert NaN and +/-Inf for weights"""
        def add_extremes(row):
            new_val = {"Charizard": np.inf,
                       "Squirtle": -np.inf,
                       "Wartortle": np.nan}
            return new_val.get(row["item"], row['weight'])

        data_stat = self.get_file("mode_stats.tsv")
        schema_stat = [("weight", dtypes.float64), ("item", str)]
        stat_frame = self.context.frame.import_csv(
            data_stat, schema=schema_stat, delimiter="\t")
        stat_frame.add_columns(add_extremes, ("weight2", dtypes.float64))
        stats = stat_frame.column_mode(
            "item", "weight2", max_modes_returned=50)

        self.assertEqual(stats.mode_count, 5)
        self.assertEqual(stats.total_weight, 1743)
        self.assertIn("Scrafty", stats.modes)

    def test_extreme_col_summary(self):
        """ Test column_summary_stats with Inf / NaN data """
        # Add a new column; replace some values with +/-Inf or NaN
        def add_extremes(row):
            new_val = {20: np.inf,
                       30: -np.inf,
                       40: np.nan}
            return new_val.get(row.Item, row.Item)

        data_mode = self.get_file("mode_stats2.csv")
        schema_mode = [("Item", dtypes.int32), ("Weight", dtypes.int32)]
        stat_frame = self.context.frame.import_csv(
            data_mode, schema=schema_mode)

        # Create new column where only values 10 and 50 are valid;
        stat_frame.add_columns(add_extremes, ("Item2", dtypes.float64))
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
            bf = dtypes.float64(2) ** 1021
            new_val = {600: 30,
                       1: 0,
                       2: bf * 2,
                       3: dtypes.float64(2) ** 1023,
                       3.1: 5.4}
            return new_val.get(row.Factors, row.Factors)

        data_corner = self.get_file("SummaryStats2.csv")
        schema_corner = [("Item", dtypes.float64), ("Factors", dtypes.float64)]

        stat_frame = self.context.frame.import_csv(
            data_corner, schema=schema_corner)

        stat_frame.add_columns(add_extremes, ("Weight", dtypes.float64))
        stat_frame.drop_columns("Factors")
        stats = stat_frame.column_summary_statistics(
                           "Item", weights_column="Weight")

        self.assertEqual(stats.mean, 1624.6)
        self.assertEqual(stats.good_row_count, 56)
        self.assertAlmostEqual(stats.variance, 563700.64)

        stat_frame.drop_rows(lambda row: row)  # Just drops all rows
        stats = stat_frame.column_summary_statistics(
                           "Item", weights_column="Weight")

        self.assertTrue(math.isnan(stats.maximum))
        self.assertEqual(stats.good_row_count, 0)
        self.assertEqual(stats.geometric_mean, 1.0)


if __name__ == "__main__":
    unittest.main()
