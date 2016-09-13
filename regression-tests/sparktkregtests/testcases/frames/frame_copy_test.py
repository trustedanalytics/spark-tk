"""Tests frame_copy with various parameters """

import unittest
import sys
import os
from sparktkregtests.lib import sparktk_test


class FrameCopyTest(sparktk_test.SparkTKTestCase):

    def setUp(self):
        """Build test frame"""
        super(FrameCopyTest, self).setUp()

        dataset = self.get_file("AddCol01.csv")
        schema = [("alpha", int),
                  ("beta", int),
                  ("gamma", int)]

        self.frame = self.context.frame.import_csv(
            dataset, schema=schema)


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
        column_list = ["alpha", "beta", "gamma"]
        copy_frame = self.frame.copy(columns=column_list)
        self.assertFramesEqual(copy_frame, self.frame)

    def test_copy_all_renamed(self):
        """Test copying while renaming all columns."""
        rename_dict = {"alpha": "alpha",
                       "beta": "bravo",
                       "gamma": "charlie"}
        copy_frame = self.frame.copy(columns=rename_dict)
        self.assertFramesEqual(copy_frame, self.frame)

    def test_copy_where(self):
        """Test copy with column rename and 'where' function"""
        rename_dict = {"alpha": "Alpha",
                       "beta": "Beta"}
        copy_frame = self.frame.copy(columns=rename_dict,
                                     where=lambda row: row.gamma > 5)

        self.assertEqual(copy_frame.count()+8, self.frame.count())
        self.frame.filter(lambda x: x.gamma > 5)
        self.assertFramesEqual(copy_frame, self.frame.copy(rename_dict.keys()))
        self.assertIn("Alpha", copy_frame.column_names)
        self.assertNotIn("beta", copy_frame.column_names)
        self.assertNotIn("gamma", copy_frame.column_names)

    def test_take_and_inspect(self):
        """Test take and inspect pull frames in the same order"""
        col_choice = ["beta", "gamma"]
        copy_frame = self.frame.copy(columns=col_choice)
        self.assertItemsEqual(
            copy_frame.take(copy_frame.count()),
            self.frame.take(self.frame.count(), columns=col_choice))

if __name__ == "__main__":
    unittest.main()
