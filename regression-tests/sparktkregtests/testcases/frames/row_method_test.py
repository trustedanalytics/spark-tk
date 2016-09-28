"""test drop_rows, filter, append, flatten_column, drop_duplicates, unicode"""

import unittest
import numpy as np

from sparktkregtests.lib import sparktk_test


class RowMethodTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build basic row"""
        super(RowMethodTest, self).setUp()

        datafile_drop = self.get_file("AddCol02.csv")
        schema_drop = [("col_A", int),
                       ("col_B", int),
                       ("col_C", int)]

        self.drop_frame = self.context.frame.import_csv(
            datafile_drop, schema=schema_drop)

        datafile_unicode = self.get_file("unicode01.csv")
        schema_unicode = [("Text_A", str),
                          ("col_B", int),
                          ("col_C", float)]
        self.unicode_frame = self.context.frame.import_csv(
            datafile_unicode, schema=schema_unicode)

        datafile_flatten = self.get_file("stackedNums.csv")
        schema_flatten = [("col_A", int),
                          ("col_B", str),
                          ("col_C", int)]

        self.flatten_frame = self.context.frame.import_csv(
            datafile_flatten, schema=schema_flatten)

        self.append_file_1col = self.get_file("append_example_1col.csv")
        self.append_schema_1col = [("col_1", str)]

        self.append_file_2col = self.get_file("append_example_2col.csv")
        self.append_schema_2col = [("col_1", str), ("col_qty", int)]

    def test_drop_lambda_simple(self):
        """ drop_rows with a simple lambda expression """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.drop_rows(lambda row: row['col_B'] < 5)

        self.assertEqual(self.drop_frame.count(), 23)

    def test_drop_lambda_compound(self):
        """ drop_rows with a compound lambda expression """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.drop_rows(
            lambda row: row['col_A'] == 2 or row['col_C'] > 50)

        self.assertEqual(self.drop_frame.count(), 17)

    def test_drop_function(self):
        """ drop_rows with a function as predicate """
        def naturals_only(row):
            """filter/drop predicate"""
            return row['col_A'] <= 0 or row['col_B'] <= 0 or row['col_C'] <= 0

        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.drop_rows(naturals_only)
        self.assertEqual(self.drop_frame.count(), 27)

    def test_drop_all_none(self):
        """ drop no  rows drop all rows """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.drop_rows(lambda row: row['col_A'] == 3.14159)
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.drop_rows(lambda row: row['col_A'] > -1000)
        self.assertEqual(self.drop_frame.count(), 0)

    def test_filter_lambda_simple(self):
        """ filter with a simple lambda expression """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.filter(lambda row: row['col_B'] < 5)
        self.assertEqual(self.drop_frame.count(), 10)

    def test_filter_lambda_compound(self):
        """ filter with a compound lambda expression """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.filter(lambda row: row['col_A'] == 2 or
                               row['col_C'] > 50)
        self.assertEqual(self.drop_frame.count(), 16)

    def test_filter_function(self):
        """ filter with a function as predicate """
        def naturals_only(row):
            """filter/drop predicate"""
            return row['col_A'] <= 0 or row['col_B'] <= 0 or row['col_C'] <= 0

        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.filter(naturals_only)
        self.assertEqual(self.drop_frame.count(), 6)

    def test_filter_all_none(self):
        """ filter no  rows filter all rows """
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.filter(lambda row: row['col_A'] > -1000)
        self.assertEqual(self.drop_frame.count(), 33)

        self.drop_frame.filter(lambda row: row['col_A'] == 3.14159)
        self.assertEqual(self.drop_frame.count(), 0)

    def test_flatten_basic(self):
        """ flatten comma-separated rows """
        self.assertEqual(self.flatten_frame.count(), 11)

        # Verify that we can flatten rows with NaN and +/-Inf values.
        self.flatten_frame.flatten_columns('col_B')
        self.assertEqual(self.flatten_frame.count(), 34)

    def test_flatten_extreme_vals(self):
        """ flatten comma-separated rows """
        def make_it_interesting(row):
            if row["col_A"] == 1:
                return np.inf
            if row["col_A"] == 2:
                return -np.inf
            if row["col_A"] == 3:
                return np.nan
            return row["col_A"]

        self.assertEqual(self.flatten_frame.count(), 11)

        # Verify that we can flatten rows with NaN and +/-Inf values.
        self.flatten_frame.add_columns(make_it_interesting,
                                       ("col_D", float))
        self.flatten_frame.flatten_columns('col_B')
        self.assertEqual(self.flatten_frame.count(), 34)

    def test_drop_dup_basic(self):
        """ Drop duplicates in a mixed-duplicate frame """
        self.assertEqual(self.drop_frame.count(), 33)

        # Some rows have multiple duplicates; drop those
        # Some rows have no duplicates
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.count(), 21)

    def test_append_drop_dup(self):
        """drop duplicates, append frame to self, drop again"""
        self.assertEqual(self.drop_frame.count(), 33)

        # Drop initial duplicates
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        orig_count = self.drop_frame.count()
        self.assertEqual(self.drop_frame.count(), 21)

        # Now there are no duplicates; drop
        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.count(), orig_count)

        # Append frame to self; each is duplicated exactly once
        self.drop_frame.append(self.drop_frame)
        self.assertEqual(self.drop_frame.count(), 2 * orig_count)

        self.drop_frame.drop_duplicates(["col_A", "col_B", "col_C"])
        self.assertEqual(self.drop_frame.count(), orig_count)

    def test_append_example(self):
        """run example append """
        my_frame = self.context.frame.import_csv(
            self.append_file_1col, schema=self.append_schema_1col)
        your_frame = self.context.frame.import_csv(
            self.append_file_2col, schema=self.append_schema_2col)
        my_frame.append(your_frame)

        self.assertEqual(8, my_frame.count())

        append_take = my_frame.take(my_frame.count())
        frame_col1_list = [x[0] for x in append_take]
        dog_row = frame_col1_list.index("dog")

        self.assertIsNone(append_take[dog_row][-1])

    def test_row_unicode_add(self):
        """ Test row functions with Unicode data """
        self.assertEqual(self.unicode_frame.count(), 32)

        self.unicode_frame.drop_rows(lambda row:
                                     row.Text_A.endswith(u'\u24CE'))
        self.assertEqual(self.unicode_frame.count(), 31)

        def insert_none(row):
            """Insert None at every 6th row"""
            new_val = row["col_B"]
            if row["col_B"] % 6 == 0:
                return None
            return new_val

        self.unicode_frame.add_columns(insert_none,
                                       schema=("col_B2", int))
        self.assertIn("col_B2", self.unicode_frame.column_names)

        self.unicode_frame.filter(lambda row: row.col_B2 is not None)
        self.assertEqual(self.unicode_frame.count(), 26)

    def test_row_unicode_drop(self):
        """ Test row functions with Unicode data """
        self.assertEqual(self.unicode_frame.count(), 32)

        def build_concat(row):
            """Create new column by concatenating several others:"""
            new_val = str(row["col_C"]) + " " + row.Text_A \
                + " - " + str(row["col_B"])
            if row["col_B"] % 7 == 0:
                new_val = None
            return new_val

        self.unicode_frame.add_columns(build_concat, schema=("Concat", str))
        self.assertIn("Concat", self.unicode_frame.column_names)

        float_frame = self.unicode_frame.copy({"col_C": "float_C"})
        self.assertEqual(float_frame.count(), 32)

        self.unicode_frame.drop_columns("Text_A")
        self.assertNotIn("Text_A", self.unicode_frame.column_names)
        self.assertEqual(float_frame.count(), 32)


if __name__ == "__main__":
    unittest.main()
