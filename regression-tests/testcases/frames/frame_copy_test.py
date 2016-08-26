"""Test frame copy """

import unittest

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
from qalib import sparktk_test

class FrameCopyTest(sparktk_test.SparkTKTestCase):
    def setUp(self):
        """Create test frames"""
        super(FrameCopyTest, self).setUp()
        dataset = self.get_file("AddCol01.csv")
        schema = [("col_A", int),
                  ("col_B", int),
                  ("col_C", int)]
        self.frame = self.context.frame.import_csv(dataset, schema=schema)

    def test_frame_copy_default(self):
        """Validate explicit copy with frame.copy()"""
        copy_frame = self.frame.copy()
        self.assertFramesEqual(copy_frame, self.frame)

    def test_copy_column_default(self):
        """Test the default column copy functionality."""
        copy_frame = self.frame.copy(columns=None)
        self.assertFramesEqual(self.frame, copy_frame)

    def test_copy_all_columns(self):
        """Test the copy all column functionality."""
        column_list = ["col_A", "col_B", "col_C"]
        copy_frame = self.frame.copy(columns=column_list)
        self.assertFramesEqual(copy_frame, self.frame)

    def test_copy_all_renamed(self):
        """Test copying while renaming all columns."""
        rename_dict = {"col_A": "alpha",
                       "col_B": "bravo",
                       "col_C": "charlie"}
        copy_frame = self.frame.copy(columns=rename_dict)
        self.assertFramesEqual(copy_frame, self.frame)

    def test_copy_where(self):
        """Test copy with column rename and 'where' function"""
        rename_dict = {"col_A": "alpha",
                       "col_B": "beta"}
        copy_frame = self.frame.copy(columns=rename_dict,
                                     where=lambda row: row.col_B > 3)

        self.assertEqual(copy_frame.row_count+4, self.frame.row_count)
        self.frame.filter(lambda x: x.col_B > 3)
        self.assertFramesEqual(copy_frame, self.frame.copy(rename_dict.keys()))
        self.assertIn("alpha", copy_frame.column_names)
        self.assertNotIn("col_B", copy_frame.column_names)
        self.assertNotIn("col_C", copy_frame.column_names)

    def test_take_and_inspect(self):
        """Test take and inspect pull frames in the same order"""
        col_choice = ["col_B", "col_C"]
        copy_frame = self.frame.copy(columns=col_choice)
        self.assertItemsEqual(
            copy_frame.take(self.frame.row_count),
            self.frame.take(self.frame.row_count, columns=col_choice))


    def test_predicated_copy(self):
        """ Test copy with a where/lambda clause"""
        data_pred = self.get_file('uniqueVal-rand2M-10KRecs.csv')
        schema_pred = [('datapoint', str), ('randval', int)]
        frame_pred = self.context.frame.import_csv(data_pred, schema=schema_pred)

        half = frame_pred.copy(where=lambda row: row.randval % 2 == 0)

        self.assertAlmostEqual(
            float(half.row_count) / float(frame_pred.row_count),
            0.5, delta=.05)

        frame_pred.filter(lambda x: x.randval % 2 == 0)

        self.assertFramesEqual(frame_pred, half)

if __name__ == "__main__":
    unittest.main()
